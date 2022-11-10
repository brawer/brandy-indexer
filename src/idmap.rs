// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>

use byteorder::{ReadBytesExt, WriteBytesExt};
use extsort::{ExternalSorter, Sortable};
use memmap::{Mmap, MmapOptions};
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct IdPair(pub u64, pub u64);

impl Sortable for IdPair {
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

pub struct IdMap<'a> {
    _mmap: Mmap,
    data: &'a [IdPair],
}

impl<'a> IdMap<'a> {
    pub fn open(path: &Path) -> std::io::Result<IdMap> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let data_ptr = mmap.as_ptr() as *const IdPair;
        let data_len = mmap.len() / mem::size_of::<IdPair>();
        let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
        Ok(IdMap {
            _mmap: mmap,
            data: data,
        })
    }

    pub fn build(workdir: &Path, filename: &str) -> (SyncSender<IdPair>, thread::JoinHandle<()>) {
        let (tx, rx) = sync_channel::<IdPair>(64 * 1024);
        let mut sort_dir = PathBuf::from(workdir);
        sort_dir.push(format!("sort-{filename}"));
        if sort_dir.try_exists().ok() == Some(true) {
            fs::remove_dir_all(sort_dir.as_path()).expect("cannot delete pre-existing sort_dir");
        }
        fs::create_dir_all(sort_dir.as_path()).expect("cannot create sort_dir");

        let filename = filename.to_string();
        let sort_dir = sort_dir.clone();
        let mut output_path = PathBuf::from(workdir);
        output_path.push(filename);
        let join_handle = thread::spawn(move || {
            let file = fs::File::create(output_path).unwrap();
            let mut writer = BufWriter::with_capacity(64 * 1024, file);
            let sorter = ExternalSorter::new()
                .with_sort_dir(sort_dir.clone())
                .with_segment_size(1024 * 1024);
            for pair in sorter.sort(rx.iter()).unwrap() {
                pair.encode(&mut writer);
            }
            fs::remove_dir_all(sort_dir.as_path()).expect("cannot delete sort_dir");
        });

        (tx, join_handle)
    }

    pub fn get(&self, key: u64) -> IdMapIter {
        let pos = self.data.partition_point(|x| x.0 < key);
        IdMapIter {
            map: self.data,
            key,
            pos,
        }
    }

    pub fn _keys(&self) -> IdMapKeyIter {
        IdMapKeyIter {
            data: self.data,
            pos: 0,
        }
    }

    pub fn iter(&self) -> IdMapEntriesIter {
        IdMapEntriesIter {
            data: self.data,
            pos: 0,
        }
    }
}

pub struct IdMapKeyIter<'a> {
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

pub struct IdMapIter<'a> {
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

pub struct IdMapEntriesIter<'a> {
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

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn make_idmap(data: &[IdPair]) -> IdMap {
        let mmap_mut = MmapOptions::new().len(4096).map_anon().unwrap();
        let _mmap = mmap_mut.make_read_only().unwrap();
        IdMap { _mmap, data }
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
        assert_eq!(idmap._keys().collect::<Vec<u64>>(), &[4, 7, 9]);
    }
}
