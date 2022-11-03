// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>

use bitvec::prelude::{BitVec, Lsb0};
use byteorder::{ReadBytesExt, WriteBytesExt};
use clap::Parser;
use extsort::ExternalSorter;
use memmap::{Mmap, MmapOptions};
use osmpbf::{BlobDecode, BlobReader, PrimitiveBlock, RelMemberType};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Mutex;
use std::thread;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to OpenStreetMap planet dump in protocol buffer format
    #[arg(short, long, value_name = "planet.pbf")]
    planet: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    if false {
        RelPhase::run(cli.planet.as_path())?;
    }

    println!("start pruning reltree");
    let tree = IdMap::open(Path::new("reltree_all"))?;
    let mut count = 0;
    let mut pruned_count = 0;
    for rel in tree.keys() {
        count += 1;
        if keep_rel(&tree, rel) {
            pruned_count += 1;
        }
    }
    let size_mb = pruned_count * 16 / (1024 * 1024);
    println!(
        "pruned reltree from {} to {} nodes; new size: {}M",
        count, pruned_count, size_mb
    );

    let relways = IdMap::open(Path::new("relways_all"))?;
    let mut count = 0;
    let mut pruned_count = 0;
    for entry in relways.iter() {
        let (way, rel) = (entry.0, entry.1);
        count += 1;
        if keep_rel(&tree, rel) {
            pruned_count += 1;
        }
    }
    let size_mb = pruned_count * 16 / (1024 * 1024);
    println!(
        "pruned relways from {} to {} nodes; new size: {}M",
        count, pruned_count, size_mb
    );

    Ok(())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct IdPair(u64, u64);

impl extsort::Sortable for IdPair {
    fn encode<W: Write>(&self, write: &mut W) {
        write.write_u64::<byteorder::LittleEndian>(self.0).unwrap();
        write.write_u64::<byteorder::LittleEndian>(self.1).unwrap();
    }

    fn decode<R: Read>(read: &mut R) -> Option<IdPair> {
        let a = read.read_u64::<byteorder::LittleEndian>().ok()?;
        let b = read.read_u64::<byteorder::LittleEndian>().ok()?;
        Some(IdPair(a, b))
    }
}

struct RelPhase {
    reltree: SyncSender<IdPair>,  // child rel -> parent rel
    relnodes: SyncSender<IdPair>, // rel -> child node
    relways: SyncSender<IdPair>,  // rel -> child way
}

fn build_pairs(filename: &str) -> (SyncSender<IdPair>, thread::JoinHandle<()>) {
    let (tx, rx) = sync_channel::<IdPair>(64 * 1024);
    let sort_dir: PathBuf = ["tmp", filename].iter().collect();
    if sort_dir.try_exists().ok() == Some(true) {
        fs::remove_dir_all(sort_dir.as_path()).expect("cannot delete pre-existing sort_dir");
    }
    fs::create_dir_all(sort_dir.as_path()).expect("cannot create sort_dir");

    let filename = filename.to_string();
    let sort_dir = sort_dir.clone();
    let join_handle = thread::spawn(move || {
        use extsort::Sortable;
        let file = fs::File::create(filename).unwrap();
        let mut buffer = BufWriter::with_capacity(64 * 1024, file);
        let sorter = ExternalSorter::new()
            .with_sort_dir(sort_dir.clone())
            .with_segment_size(1024 * 1024);
        for pair in sorter.sort(rx.iter()).unwrap() {
            pair.encode(&mut buffer);
        }
        fs::remove_dir_all(sort_dir.as_path()).expect("cannot delete sort_dir");
    });

    (tx, join_handle)
}

impl RelPhase {
    fn run(path: &Path) -> Result<(), Box<dyn Error>> {
        let (reltree, reltree_worker) = build_pairs("reltree_all");
        let (relnodes, relnodes_worker) = build_pairs("relnodes_all");
        let (relways, relways_worker) = build_pairs("relways_all");
        let mut phase = RelPhase {
            reltree,
            relnodes,
            relways,
        };
        let m = Mutex::new(&mut phase);
        let blobs = BlobReader::from_path(path)?;
        blobs
            .into_iter()
            .par_bridge()
            .try_for_each(|b| match b?.decode() {
                Ok(BlobDecode::OsmData(block)) => RelPhase::process(&m, block),
                Ok(BlobDecode::OsmHeader(_)) => Ok(()),
                Ok(BlobDecode::Unknown(_)) => Ok(()),
                Err(e) => Err(e),
            })?;

        // Close channels. This tells workers there’s no more data coming.
        drop(phase.reltree);
        drop(phase.relnodes);
        drop(phase.relways);

        // Wait for completion of workers.
        reltree_worker.join().expect("reltree_worker failed");
        relnodes_worker.join().expect("relnodes_worker failed");
        relways_worker.join().expect("relways_worker failed");

        Ok(())
    }

    fn process(phase: &Mutex<&mut Self>, block: PrimitiveBlock) -> Result<(), osmpbf::Error> {
        let reltree;
        let relnodes;
        let relways;
        {
            let phase = phase.lock().unwrap();
            reltree = phase.reltree.clone();
            relnodes = phase.relnodes.clone();
            relways = phase.relways.clone();
        }

        let matcher = TagMatcher::new(&block);
        for group in block.groups() {
            for rel in group.relations() {
                let rel_id = rel.id() as u64;
                for m in rel.members() {
                    let member_id = m.member_id as u64;
                    match m.member_type {
                        RelMemberType::Node => {
                            relnodes.send(IdPair(rel_id, member_id)).unwrap();
                        }
                        RelMemberType::Way => {
                            relways.send(IdPair(rel_id, member_id)).unwrap();
                        }
                        RelMemberType::Relation => {
                            reltree.send(IdPair(member_id, rel_id)).unwrap();
                        }
                    }
                }
                if matcher.is_interesting(rel.raw_tags()) {
                    let pair = IdPair(rel.id() as u64, 0);
                    reltree.send(pair).unwrap();
                }
            }
        }

        Ok(())
    }
}

struct IdMap<'a> {
    mmap: Mmap,
    data: &'a [IdPair],
}

impl<'a> IdMap<'a> {
    fn open(path: &Path) -> std::io::Result<IdMap> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let data_ptr = mmap.as_ptr() as *const IdPair;
        let data_len = mmap.len() / mem::size_of::<IdPair>();
        let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
        Ok(IdMap { mmap, data })
    }

    fn contains_key(&self, key: &u64) -> bool {
        return self.data.binary_search_by_key(key, |x| x.0).is_ok();
    }

    fn get(&self, key: u64) -> IdMapIter {
        let pos = self.data.partition_point(|x| x.0 < key);
        IdMapIter {
            map: self.data,
            key,
            pos,
        }
    }

    fn keys(&self) -> IdMapKeyIter {
        IdMapKeyIter {
            data: self.data,
            pos: 0,
        }
    }

    fn iter(&self) -> IdMapEntriesIter {
        IdMapEntriesIter {
            data: self.data,
            pos: 0,
        }
    }
}

struct IdMapKeyIter<'a> {
    data: &'a [IdPair],
    pos: usize,
}

impl<'a> Iterator for IdMapKeyIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let limit = self.data.len();
        if self.pos >= limit {
            return None;
        }
        let key = self.data[self.pos].0;
        while self.pos < limit && self.data[self.pos].0 == key {
            self.pos += 1
        }
        return Some(key);
    }
}

struct IdMapIter<'a> {
    map: &'a [IdPair],
    key: u64,
    pos: usize,
}

impl<'a> Iterator for IdMapIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.map.len() {
            return None;
        }
        let entry = &self.map[self.pos];
        if entry.0 != self.key {
            return None;
        }
        self.pos += 1;
        return Some(entry.1);
    }
}

struct IdMapEntriesIter<'a> {
    data: &'a [IdPair],
    pos: usize,
}

impl<'a> Iterator for IdMapEntriesIter<'a> {
    type Item = &'a IdPair;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let entry = &self.data[self.pos];
        self.pos += 1;
        Some(entry)
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

struct TagMatcher {
    wikidata_keys: BitVec,
    nsi_keys: BitVec,
    nsi_values: BitVec,
}

impl TagMatcher {
    fn new(block: &PrimitiveBlock) -> TagMatcher {
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
            if is_nsi_key(s) {
                nsi_keys.set(i, true);
            }
            if is_nsi_value(s) {
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

fn is_nsi_key(key: &str) -> bool {
    key == "brand"
        || key.starts_with("brand:")
        || key == "name"
        || key.starts_with("name:")
        || key == "network"
        || key.starts_with("network:")
        || key == "operator"
        || key.starts_with("operator:")
}

fn is_nsi_value(v: &str) -> bool {
    v == "Starbucks" || v == "Brezelkönig" || v == "Müller" || v == "ZVV"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_idmap(data: &[IdPair]) -> IdMap {
        let mmap_mut = MmapOptions::new().len(4096).map_anon().unwrap();
        let mmap = mmap_mut.make_read_only().unwrap();
        IdMap { mmap, data }
    }

    #[test]
    fn idmap_keys() {
        let idmap = make_idmap(&[
            IdPair(4, 444),
            IdPair(7, 0),
            IdPair(7, 1),
            IdPair(7, 2),
            IdPair(7, 3),
            IdPair(9, 99),
        ]);
        assert!(idmap.keys().collect::<Vec<u64>>() == &[4, 7, 9]);
    }

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
}
