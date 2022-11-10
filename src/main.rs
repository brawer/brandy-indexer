// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>

use bitvec::prelude::{BitVec, Lsb0};
use clap::Parser;
use extsort::Sortable;
use osmpbf::{Blob, BlobDecode, BlobReader, PrimitiveBlock, RelMemberType};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::sync::Mutex;
use std::time::SystemTime;

mod idmap;
use crate::idmap::{IdMap, IdPair};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to OpenStreetMap planet dump in protocol buffer format
    #[arg(short, long, value_name = "planet.pbf")]
    planet: PathBuf,

    /// Path to working directory for storing temporary files
    #[arg(short, long, value_name = "workdir", default_value = "workdir")]
    workdir: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let dump = PlanetDump::open(cli.planet.as_path())?;
    let config = TagMatcherConfig::new();
    let workdir = cli.workdir.as_path();
    RelPhase::run(&dump, &config, workdir)?;
    Ok(())
}

struct PlanetDump {
    node_blobs: Vec<Blob>,
    way_blobs: Vec<Blob>,
    rel_blobs: Vec<Blob>,
}

impl PlanetDump {
    fn open(path: &Path) -> osmpbf::Result<PlanetDump> {
        let start = SystemTime::now();
        let mut blobs: Vec<Blob> = Vec::new();
        for b in BlobReader::from_path(path)? {
            blobs.push(b?);
        }

        // Partition planet dump into (node_blobs, way_blobs, rel_blobs).
        let (left, rel_blobs): (Vec<Blob>, Vec<Blob>) = blobs.into_par_iter().partition(|b| {
            if let Ok(BlobDecode::OsmData(block)) = b.decode() {
                !block.groups().any(|g| g.relations().next().is_some())
            } else {
                true
            }
        });
        let (node_blobs, way_blobs): (Vec<Blob>, Vec<Blob>) = left.into_par_iter().partition(|b| {
            if let Ok(BlobDecode::OsmData(block)) = b.decode() {
                !block.groups().any(|g| g.ways().next().is_some())
            } else {
                true
            }
        });

        println!(
            "PlanetDump opened in {:.1} secs",
            start.elapsed().unwrap().as_secs_f32()
        );

        Ok(PlanetDump {
            node_blobs,
            way_blobs,
            rel_blobs,
        })
    }
}

struct RelPhase<'a> {
    tag_matcher_config: &'a TagMatcherConfig<'a>,
    reltree: SyncSender<IdPair>,       // child rel → parent rel
    nodes_in_rels: SyncSender<IdPair>, // child node → parent rel
    ways_in_rels: SyncSender<IdPair>,  // child way → parent rel
}

impl<'a> RelPhase<'a> {
    fn run(
        dump: &PlanetDump,
        tag_matcher_config: &TagMatcherConfig,
        workdir: &Path,
    ) -> Result<(), Box<dyn Error>> {
        let phase_start = SystemTime::now();
        let (reltree, reltree_worker) = IdMap::build(workdir, "reltree_all");
        let (nodes_in_rels, nodes_in_rels_worker) = IdMap::build(workdir, "nodes_in_rels_all");
        let (ways_in_rels, ways_in_rels_worker) = IdMap::build(workdir, "ways_in_rels_all");
        let mut phase = RelPhase {
            tag_matcher_config,
            reltree,
            nodes_in_rels,
            ways_in_rels,
        };
        let m = Mutex::new(&mut phase);

        dump.rel_blobs
            .iter()
            .par_bridge()
            .try_for_each(|b| match b.decode() {
                Ok(BlobDecode::OsmData(block)) => RelPhase::process(&m, block),
                Ok(BlobDecode::OsmHeader(_)) => Ok(()),
                Ok(BlobDecode::Unknown(_)) => Ok(()),
                Err(e) => Err(e),
            })?;

        // Close channels. This tells workers there’s no more data coming.
        drop(phase.reltree);
        drop(phase.nodes_in_rels);
        drop(phase.ways_in_rels);

        // Wait for completion of workers.
        reltree_worker.join().expect("reltree_worker failed");
        nodes_in_rels_worker
            .join()
            .expect("nodes_in_rels_worker failed");
        ways_in_rels_worker
            .join()
            .expect("ways_in_rels_worker failed");

        Self::prune_reltree(workdir)?;
        Self::prune_ways_in_rels(workdir)?;
        Self::prune_nodes_in_rels(workdir)?;
        println!(
            "RelPhase finished in {:.1} secs",
            phase_start.elapsed().unwrap().as_secs_f32()
        );

        Ok(())
    }

    fn process(phase: &Mutex<&mut Self>, block: PrimitiveBlock) -> Result<(), osmpbf::Error> {
        let tag_matcher_config;
        let reltree;
        let nodes_in_rels;
        let ways_in_rels;
        {
            let phase = phase.lock().unwrap();
            tag_matcher_config = phase.tag_matcher_config;
            reltree = phase.reltree.clone();
            nodes_in_rels = phase.nodes_in_rels.clone();
            ways_in_rels = phase.ways_in_rels.clone();
        }

        let matcher = TagMatcher::new(&block, tag_matcher_config);
        for group in block.groups() {
            for rel in group.relations() {
                let rel_id = rel.id() as u64;
                for m in rel.members() {
                    let pair = IdPair(m.member_id as u64, rel_id);
                    match m.member_type {
                        RelMemberType::Node => nodes_in_rels.send(pair),
                        RelMemberType::Way => ways_in_rels.send(pair),
                        RelMemberType::Relation => reltree.send(pair),
                    }
                    .unwrap();
                }
                if matcher.is_interesting(rel.raw_tags()) {
                    reltree.send(IdPair(rel_id, 0)).unwrap();
                }
            }
        }

        Ok(())
    }

    fn prune_reltree(workdir: &Path) -> Result<(), Box<dyn Error>> {
        let mut reltree_all_path = PathBuf::from(workdir);
        reltree_all_path.push("reltree_all");

        let mut reltree_path = PathBuf::from(workdir);
        reltree_path.push("reltree");
        let reltree_file = fs::File::create(reltree_path).unwrap();
        let mut writer = BufWriter::with_capacity(64 * 1024, reltree_file);

        let tree = IdMap::open(&reltree_all_path)?;
        let mut last_keep_for_itself = 0;
        for pair in tree.iter() {
            let (child, parent) = (pair.0, pair.1);
            if parent == 0 {
                last_keep_for_itself = child;
                pair.encode(&mut writer);
            } else if child != last_keep_for_itself && keep_rel(&tree, parent) {
                pair.encode(&mut writer);
            }
        }
        Ok(())
    }

    fn prune_ways_in_rels(workdir: &Path) -> Result<(), Box<dyn Error>> {
        let reltree_path = join_path(workdir, "reltree");
        let reltree = IdMap::open(&reltree_path)?;
        let ways_in_rels_all_path = join_path(workdir, "ways_in_rels_all");
        let ways_in_rels_all = IdMap::open(&ways_in_rels_all_path)?;

        let mut writer = {
            let mut path = PathBuf::from(workdir);
            path.push("ways_in_rels");
            let out_file = fs::File::create(path).unwrap();
            BufWriter::with_capacity(64 * 1024, out_file)
        };

        for pair in ways_in_rels_all.iter() {
            let (_way, rel) = (pair.0, pair.1);
            if keep_rel(&reltree, rel) {
                pair.encode(&mut writer);
            }
        }

        Ok(())
    }

    fn prune_nodes_in_rels(workdir: &Path) -> Result<(), Box<dyn Error>> {
        let reltree_path = join_path(workdir, "reltree");
        let reltree = IdMap::open(&reltree_path)?;
        let nodes_in_rels_all_path = join_path(workdir, "nodes_in_rels_all");
        let nodes_in_rels_all = IdMap::open(&nodes_in_rels_all_path)?;

        let mut writer = {
            let mut path = PathBuf::from(workdir);
            path.push("nodes_in_rels");
            let out_file = fs::File::create(path).unwrap();
            BufWriter::with_capacity(64 * 1024, out_file)
        };

        for pair in nodes_in_rels_all.iter() {
            let (_node, rel) = (pair.0, pair.1);
            if keep_rel(&reltree, rel) {
                pair.encode(&mut writer);
            }
        }

        Ok(())
    }
}

fn keep_rel(reltree: &IdMap, rel: u64) -> bool {
    let mut chain = Vec::<u64>::with_capacity(8);
    return _keep_rel(reltree, rel, &mut chain);
}

fn _keep_rel(reltree: &IdMap, rel: u64, chain: &mut Vec<u64>) -> bool {
    let mut iter = reltree.get(rel);
    while let Some(p) = iter.next() {
        if p == 0 {
            return true;
        }
        chain.push(rel); // detect cycles in OSM relation tree
        if !chain.contains(&p) {
            if _keep_rel(reltree, p, chain) {
                return true;
            }
        }
        chain.pop();
    }
    return false;
}

// Placeholder for NSI configuration, country boundaries, and other data
// needed for deciding whether we're interested in a tagged OpenStreetMap
// node/way/relation. For now, we just look for a few hardcoded names;
// in the long term, this will probably evolve into a fast implementation
// of NSI,the OpenStreetMap Name Suggestion Index. See also https://nsi.guide/.
struct TagMatcherConfig<'a> {
    nsi_names: Vec<&'a str>,
}

impl<'a> TagMatcherConfig<'a> {
    fn new() -> TagMatcherConfig<'a> {
        let nsi_names = vec!["Brezelkönig", "Starbucks", "Müller", "ZVV"];
        TagMatcherConfig { nsi_names }
    }

    fn is_nsi_key(&self, key: &str) -> bool {
        key == "brand"
            || key.starts_with("brand:")
            || key == "name"
            || key.starts_with("name:")
            || key == "network"
            || key.starts_with("network:")
            || key == "operator"
            || key.starts_with("operator:")
    }

    fn is_nsi_value(&self, v: &str) -> bool {
        self.nsi_names.contains(&v)
        // v == "Starbucks" || v == "Brezelkönig" || v == "Müller" || v == "ZVV"
    }
}

struct TagMatcher {
    wikidata_keys: BitVec,
    nsi_keys: BitVec,
    nsi_values: BitVec,
}

impl TagMatcher {
    fn new(block: &PrimitiveBlock, config: &TagMatcherConfig) -> TagMatcher {
        let stringtable: Vec<&str> = block
            .raw_stringtable()
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap_or(""))
            .collect();

        let mut wikidata_keys: BitVec<usize, Lsb0> = BitVec::new();
        let mut nsi_keys: BitVec<usize, Lsb0> = BitVec::new();
        let mut nsi_values: BitVec<usize, Lsb0> = BitVec::new();

        wikidata_keys.resize(stringtable.len(), false);
        nsi_keys.resize(stringtable.len(), false);
        nsi_values.resize(stringtable.len(), false);

        for (i, s) in stringtable.iter().enumerate() {
            if is_wikidata_key(s) {
                wikidata_keys.set(i, true);
            }
            if config.is_nsi_key(s) {
                nsi_keys.set(i, true);
            }
            if config.is_nsi_value(s) {
                nsi_values.set(i, true);
            }
        }

        TagMatcher {
            wikidata_keys,
            nsi_keys,
            nsi_values,
        }
    }

    fn is_interesting<T>(&self, tags: T) -> bool
    where
        T: std::iter::Iterator<Item = (u32, u32)>,
    {
        for (k, v) in tags {
            if self.wikidata_keys[k as usize]
                || (self.nsi_keys[k as usize] && self.nsi_values[v as usize])
            {
                return true;
            }
        }
        return false;
    }
}

fn is_wikidata_key(key: &str) -> bool {
    let len = key.len();
    if len < 8 {
        false
    } else if len == 8 {
        key == "wikidata"
    } else {
        key.ends_with(":wikidata")
    }
}

fn join_path(path: &Path, filename: &str) -> PathBuf {
    let mut path = PathBuf::from(path);
    path.push(filename);
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idmap::tests::make_idmap;

    #[test]
    fn keep_rel_works() {
        // These test cases correspond to the actual data
        // in the OpenStreetMap planet dump of October 17, 2022.

        // relation/2 is not in OSM -> should not keep
        // relation/13 has wikidata tag -> should keep
        let tree = make_idmap(&[IdPair(13, 0)]);
        assert!(keep_rel(&tree, 2) == false);
        assert!(keep_rel(&tree, 13) == true);

        // relation/75 is child of relation/3873701
        // relation/3873701 has wikidata tag
        // -> should keep both
        let tree = make_idmap(&[IdPair(75, 3873701), IdPair(3873701, 0)]);
        assert!(keep_rel(&tree, 75) == true);
        assert!(keep_rel(&tree, 3873701) == true);

        // relation/2706 is child of relation/7433034
        // relation/7433034 has neither a wikidata tag nor any parent relations
        // -> should not keep either
        let tree = make_idmap(&[IdPair(2706, 7433034)]);
        assert!(keep_rel(&tree, 2706) == false);
        assert!(keep_rel(&tree, 7433034) == false);

        // relation/13987412 is child of relation/9202820
        // relation/9202820 is child of relation/13987412
        // -> should detect cycle and drop both
        let tree = make_idmap(&[IdPair(9202820, 13987412), IdPair(13987412, 9202820)]);
        assert!(keep_rel(&tree, 2706) == false);
        assert!(keep_rel(&tree, 7433034) == false);
    }

    #[test]
    fn join_path_works() {
        assert_eq!(
            join_path(&PathBuf::from("/foo/bar"), "baz").as_os_str(),
            "/foo/bar/baz"
        );
    }
}
