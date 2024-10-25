// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is forked from the redb project.
// Original source: https://github.com/cberner/redb/blob/master/benches/common.rs

use morsdb::{Mors, MorsBuilder};
#[allow(unused_imports)]
use redb::{AccessGuard, ReadableTableMetadata, TableDefinition};
#[allow(unused_imports)]
use rocksdb::{IteratorMode, TransactionDB, TransactionOptions, WriteOptions};
use sanakirja::btree::page_unsized;
use sanakirja::{Commit, RootDb};
use std::env::current_dir;
use std::{fs, process, thread};
use std::fs::{create_dir_all, File};
// use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

pub trait BenchDatabase {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    fn write_transaction(&self) -> Self::W<'_>;

    fn read_transaction(&self) -> Self::R<'_>;
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;

    #[allow(clippy::result_unit_err)]
    fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;
    // type Iterator<'out>: BenchIterator
    // where
    //     Self: 'out;

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>>;

    // fn range_from<'a>(&'a self, start: &'a [u8]) -> Self::Iterator<'a>;

    // fn len(&self) -> u64;
}

// pub trait BenchIterator {
//     type Output<'out>: AsRef<[u8]> + 'out
//     where
//         Self: 'out;

//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)>;
// }

pub struct RedbBenchDatabase<'a> {
    db: &'a redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for RedbBenchDatabase<'a> {
    type W<'db> = RedbBenchWriteTransaction where Self: 'db;
    type R<'db> = RedbBenchReadTransaction where Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn> = RedbBenchReader where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    type Output<'out> = RedbAccessGuard<'out> where Self: 'out;
    // type Iterator<'out> = RedbBenchIterator<'out> where Self: 'out;

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>> {
        self.table.get(key).unwrap().map(RedbAccessGuard::new)
    }

    // fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
    //     let iter = self.table.range(key..).unwrap();
    //     RedbBenchIterator { iter }
    // }

    // fn len(&self) -> u64 {
    //     self.table.len().unwrap()
    // }
}

pub struct RedbBenchIterator<'a> {
    iter: redb::Range<'a, &'static [u8], &'static [u8]>,
}

// impl BenchIterator for RedbBenchIterator<'_> {
//     type Output<'a> = RedbAccessGuard<'a> where Self: 'a;

//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
//         self.iter.next().map(|item| {
//             let (k, v) = item.unwrap();
//             (RedbAccessGuard::new(k), RedbAccessGuard::new(v))
//         })
//     }
// }

pub struct RedbAccessGuard<'a> {
    inner: AccessGuard<'a, &'static [u8]>,
}

impl<'a> RedbAccessGuard<'a> {
    fn new(inner: AccessGuard<'a, &'static [u8]>) -> Self {
        Self { inner }
    }
}

impl<'a> AsRef<[u8]> for RedbAccessGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        self.inner.value()
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn> = RedbBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchInserter { table }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'txn> {
    table: redb::Table<'txn, &'static [u8], &'static [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.table.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.table.remove(key).map(|_| ()).map_err(|_| ())
    }
}

pub struct SledBenchDatabase<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> SledBenchDatabase<'a> {
    pub fn new(db: &'a sled::Db, path: &'a Path) -> Self {
        SledBenchDatabase { db, db_dir: path }
    }
}

impl<'a> BenchDatabase for SledBenchDatabase<'a> {
    type W<'db> = SledBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = SledBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: self.db,
            db_dir: self.db_dir,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReadTransaction for SledBenchReadTransaction<'db> {
    type T<'txn> = SledBenchReader<'db> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        SledBenchReader { db: self.db }
    }
}

pub struct SledBenchReader<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReader for SledBenchReader<'db> {
    type Output<'out> = sled::IVec where Self: 'out;
    // type Iterator<'out> = SledBenchIterator where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }

    // fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
    //     let iter = self.db.range(key..);
    //     SledBenchIterator { iter }
    // }

    // fn len(&self) -> u64 {
    //     self.db.len() as u64
    // }
}

pub struct SledBenchIterator {
    iter: sled::Iter,
}

// impl BenchIterator for SledBenchIterator {
//     type Output<'out> = sled::IVec where Self: 'out;

//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
//         self.iter.next().map(|x| x.unwrap())
//     }
// }

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> BenchWriteTransaction for SledBenchWriteTransaction<'a> {
    type W<'txn> = SledBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SledBenchInserter { db: self.db }
    }

    fn commit(self) -> Result<(), ()> {
        self.db.flush().unwrap();
        // Workaround for sled durability
        // Fsync all the files, because sled doesn't guarantee durability (it uses sync_file_range())
        // See: https://github.com/spacejam/sled/issues/1351
        for entry in fs::read_dir(self.db_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let file = File::open(entry.path()).unwrap();
                file.sync_all().unwrap();
            }
        }
        Ok(())
    }
}

pub struct SledBenchInserter<'a> {
    db: &'a sled::Db,
}

impl<'a> BenchInserter for SledBenchInserter<'a> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.remove(key).map(|_| ()).map_err(|_| ())
    }
}

pub struct HeedBenchDatabase<'a> {
    env: &'a heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

impl<'a> HeedBenchDatabase<'a> {
    pub fn new(env: &'a heed::Env) -> Self {
        let mut tx = env.write_txn().unwrap();
        let db = env.create_database(&mut tx, None).unwrap();
        Self { env, db }
    }
}

impl<'a> BenchDatabase for HeedBenchDatabase<'a> {
    type W<'db> = HeedBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = HeedBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "lmdb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.env.write_txn().unwrap();
        Self::W { db: self.db, txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.env.read_txn().unwrap();
        Self::R { db: self.db, txn }
    }
}

pub struct HeedBenchWriteTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: heed::RwTxn<'db>,
}

impl<'db> BenchWriteTransaction for HeedBenchWriteTransaction<'db> {
    type W<'txn> = HeedBenchInserter<'txn, 'db> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        Self::W {
            db: self.db,
            txn: &mut self.txn,
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct HeedBenchInserter<'txn, 'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &'txn mut heed::RwTxn<'db>,
}

impl BenchInserter for HeedBenchInserter<'_, '_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.put(self.txn, key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.delete(self.txn, key).map(|_| ()).map_err(|_| ())
    }
}

pub struct HeedBenchReadTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: heed::RoTxn<'db>,
}

impl<'db> BenchReadTransaction for HeedBenchReadTransaction<'db> {
    type T<'txn> = HeedBenchReader<'txn, 'db> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        Self::T {
            db: self.db,
            txn: &self.txn,
        }
    }
}

pub struct HeedBenchReader<'txn, 'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &'txn heed::RoTxn<'db>,
}

impl<'txn, 'db> BenchReader for HeedBenchReader<'txn, 'db> {
    type Output<'out> = &'out [u8] where Self: 'out;
    // type Iterator<'out> = HeedBenchIterator<'out> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.db.get(self.txn, key).unwrap()
    }

    // fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
    //     let range = (Bound::Included(key), Bound::Unbounded);
    //     let iter = self.db.range(self.txn, &range).unwrap();

    //     Self::Iterator { iter }
    // }

    // fn len(&self) -> u64 {
    //     self.db.stat(self.txn).unwrap().entries as u64
    // }
}

pub struct HeedBenchIterator<'a> {
    iter: heed::RoRange<'a, heed::types::Bytes, heed::types::Bytes>,
}

// impl BenchIterator for HeedBenchIterator<'_> {
//     type Output<'out> = &'out [u8] where Self: 'out;
//
//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
//         self.iter.next().map(|x| x.unwrap())
//     }
// }

pub struct RocksdbBenchDatabase<'a> {
    db: &'a TransactionDB,
}

impl<'a> RocksdbBenchDatabase<'a> {
    pub fn new(db: &'a TransactionDB) -> Self {
        Self { db }
    }
}

impl<'a> BenchDatabase for RocksdbBenchDatabase<'a> {
    type W<'db> = RocksdbBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = RocksdbBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "rocksdb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut write_opt = WriteOptions::new();
        write_opt.set_sync(true);
        let mut txn_opt = TransactionOptions::new();
        txn_opt.set_snapshot(true);
        let txn = self.db.transaction_opt(&write_opt, &txn_opt);
        RocksdbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let snapshot = self.db.snapshot();
        RocksdbBenchReadTransaction { snapshot }
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, TransactionDB>,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    type W<'txn> = RocksdbBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        RocksdbBenchInserter { txn: &self.txn }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RocksdbBenchInserter<'a> {
    txn: &'a rocksdb::Transaction<'a, TransactionDB>,
}

impl BenchInserter for RocksdbBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.put(key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.delete(key).map_err(|_| ())
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type T<'txn> = RocksdbBenchReader<'db, 'txn> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        RocksdbBenchReader {
            snapshot: &self.snapshot,
        }
    }
}

pub struct RocksdbBenchReader<'db, 'txn> {
    snapshot: &'txn rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db, 'txn> BenchReader for RocksdbBenchReader<'db, 'txn> {
    type Output<'out> = Vec<u8> where Self: 'out;
    // type Iterator<'out> = RocksdbBenchIterator<'out> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snapshot.get(key).unwrap()
    }

    // fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
    //     let iter = self
    //         .snapshot
    //         .iterator(IteratorMode::From(key, Direction::Forward));

    //     RocksdbBenchIterator { iter }
    // }

    // fn len(&self) -> u64 {
    //     self.snapshot.iterator(IteratorMode::Start).count() as u64
    // }
}

pub struct RocksdbBenchIterator<'a> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, TransactionDB>,
}

// impl BenchIterator for RocksdbBenchIterator<'_> {
//     type Output<'out> = Box<[u8]> where Self: 'out;
//
//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
//         self.iter.next().map(|x| {
//             let x = x.unwrap();
//             (x.0, x.1)
//         })
//     }
// }

pub struct SanakirjaBenchDatabase<'a> {
    db: &'a sanakirja::Env,
}

impl<'a> SanakirjaBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a sanakirja::Env) -> Self {
        let mut txn = sanakirja::Env::mut_txn_begin(db).unwrap();
        // XXX: There's no documentation on why this method is unsafe, so let's just hope we upheld the requirements for it to be safe!
        let table = unsafe {
            sanakirja::btree::create_db_::<
                _,
                [u8],
                [u8],
                page_unsized::Page<[u8], [u8]>,
            >(&mut txn)
            .unwrap()
        };
        txn.set_root(0, table.db.into());
        txn.commit().unwrap();
        Self { db }
    }
}

impl<'a> BenchDatabase for SanakirjaBenchDatabase<'a> {
    type W<'db> = SanakirjaBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = SanakirjaBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "sanakirja"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = sanakirja::Env::mut_txn_begin(self.db).unwrap();
        SanakirjaBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = sanakirja::Env::txn_begin(self.db).unwrap();
        SanakirjaBenchReadTransaction { txn }
    }
}

pub struct SanakirjaBenchWriteTransaction<'db> {
    txn: sanakirja::MutTxn<&'db sanakirja::Env, ()>,
}

impl<'db> BenchWriteTransaction for SanakirjaBenchWriteTransaction<'db> {
    type W<'txn> = SanakirjaBenchInserter<'db, 'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.root_db(0).unwrap();
        SanakirjaBenchInserter {
            txn: &mut self.txn,
            table,
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SanakirjaBenchInserter<'db, 'txn> {
    txn: &'txn mut sanakirja::MutTxn<&'db sanakirja::Env, ()>,
    #[allow(clippy::type_complexity)]
    table: sanakirja::btree::Db_<[u8], [u8], page_unsized::Page<[u8], [u8]>>,
}

impl BenchInserter for SanakirjaBenchInserter<'_, '_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let result =
            sanakirja::btree::put(self.txn, &mut self.table, key, value)
                .map_err(|_| ())
                .map(|_| ());
        self.txn.set_root(0, self.table.db.into());
        result
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        let result =
            sanakirja::btree::del(self.txn, &mut self.table, key, None)
                .map_err(|_| ())
                .map(|_| ());
        self.txn.set_root(0, self.table.db.into());
        result
    }
}

pub struct SanakirjaBenchReadTransaction<'db> {
    txn: sanakirja::Txn<&'db sanakirja::Env>,
}

impl<'db> BenchReadTransaction for SanakirjaBenchReadTransaction<'db> {
    type T<'txn> = SanakirjaBenchReader<'db, 'txn> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.root_db(0).unwrap();
        SanakirjaBenchReader {
            txn: &self.txn,
            table,
        }
    }
}

pub struct SanakirjaBenchReader<'db, 'txn> {
    txn: &'txn sanakirja::Txn<&'db sanakirja::Env>,
    #[allow(clippy::type_complexity)]
    table: sanakirja::btree::Db_<[u8], [u8], page_unsized::Page<[u8], [u8]>>,
}

impl<'db, 'txn> BenchReader for SanakirjaBenchReader<'db, 'txn> {
    type Output<'out> = &'out [u8] where Self: 'out;
    // type Iterator<'out> = SanakirjaBenchIterator<'db, 'txn> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        sanakirja::btree::get(self.txn, &self.table, key, None)
            .unwrap()
            .map(|(_, v)| v)
    }

    // fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
    //     let iter =
    //         sanakirja::btree::iter(self.txn, &self.table, Some((key, None)))
    //             .unwrap();

    //     SanakirjaBenchIterator { iter }
    // }

    // fn len(&self) -> u64 {
    //     sanakirja::btree::iter(self.txn, &self.table, None)
    //         .unwrap()
    //         .count() as u64
    // }
}

pub struct SanakirjaBenchIterator<'db, 'txn> {
    #[allow(clippy::type_complexity)]
    iter: sanakirja::btree::Iter<
        'txn,
        sanakirja::Txn<&'db sanakirja::Env>,
        [u8],
        [u8],
        page_unsized::Page<[u8], [u8]>,
    >,
}

// impl<'db, 'txn> BenchIterator for SanakirjaBenchIterator<'db, 'txn> {
//     type Output<'out> = &'txn [u8] where Self: 'out;

//     fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
//         self.iter.next().map(|x| {
//             let x = x.unwrap();
//             (x.0, x.1)
//         })
//     }
// }
pub struct MorsBenchDatabase<'a> {
    db: &'a Mors,
}
impl<'a> MorsBenchDatabase<'a> {
    pub fn new(db: &'a Mors) -> Self {
        Self { db }
    }
}
impl BenchDatabase for MorsBenchDatabase<'_> {
    type W<'db> = MorsBenchWriteTransaction where Self: 'db;
    type R<'db> = MorsBenchReadTransaction where Self: 'db;
    fn db_type_name() -> &'static str {
        "mors"
    }
    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        MorsBenchWriteTransaction { txn }
    }
    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        MorsBenchReadTransaction { txn }
    }
}
pub struct MorsBenchWriteTransaction {
    txn: morsdb::WriteTransaction,
}
impl BenchWriteTransaction for MorsBenchWriteTransaction {
    type W<'txn> = MorsBenchInserter<'txn> where Self: 'txn;
    fn get_inserter(&mut self) -> Self::W<'_> {
        MorsBenchInserter { txn: &mut self.txn }
    }
    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|e| eprintln!("{:?}", e))
    }
}
pub struct MorsBenchInserter<'a> {
    txn: &'a mut morsdb::WriteTransaction,
}
impl BenchInserter for MorsBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let mut entry =
            morsdb::KvEntry::new(key.to_vec().into(), value.to_vec().into());
        entry.set_value_threshold(100_000);
        self.txn.set_entry(entry).map_err(|e| eprintln!("{:?}", e))
    }
    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.delete(key.to_vec().into()).map_err(|_| ())
    }
}
pub struct MorsBenchReadTransaction {
    txn: morsdb::ReadOnlyTransaction,
}
impl BenchReadTransaction for MorsBenchReadTransaction {
    type T<'txn> = MorsBenchReader<'txn> where Self: 'txn;
    fn get_reader(&self) -> Self::T<'_> {
        MorsBenchReader { txn: &self.txn }
    }
}
pub struct MorsBenchReader<'a> {
    txn: &'a morsdb::ReadOnlyTransaction,
}
impl<'a> BenchReader for MorsBenchReader<'a> {
    type Output<'out> = Vec<u8> where Self: 'out;
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.txn.get(key.to_vec().into()) {
            Ok(e) => e.value().to_vec().into(),
            Err(_) => None,
        }
    }
    // fn len(&self) -> u64 {
    //     self.txn.len()
    // }
}
const ITERATIONS: usize = 2;
const ELEMENTS: usize = 1_000_000;
// const ELEMENTS: usize = 100_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}
fn benchmark<T: BenchDatabase + Send + Sync>(
    db: T,
) -> Vec<(String, ResultType)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let db = Arc::new(db);

    let start = Instant::now();
    let mut txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    for i in 0..ELEMENTS {
        let (key, value) = gen_pair(&mut rng);
        inserter.insert(&key, &value).unwrap();
        if i % 10_000 == 0 {
            drop(inserter);
            txn.commit().unwrap();
            txn = db.write_transaction();
            inserter = txn.get_inserter();
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), ResultType::Duration(duration)));

    let start = Instant::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = gen_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        writes,
        duration.as_millis()
    );
    results.push((
        "individual writes".to_string(),
        ResultType::Duration(duration),
    ));

    let start = Instant::now();
    let batch_size = 10000;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            for _ in 0..batch_size {
                let (key, value) = gen_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::db_type_name(),
        writes,
        batch_size,
        duration.as_millis()
    );
    results.push((
        format!("batch {}*{} writes", writes, batch_size).to_string(),
        ResultType::Duration(duration),
    ));

    let txn = db.read_transaction();
    {
        {
            let start = Instant::now();
            // let len = txn.get_reader().len();
            // assert_eq!(len, ELEMENTS as u64 + 100_000 + 100);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: len() in {}ms",
                T::db_type_name(),
                duration.as_millis()
            );
            results.push(("len()".to_string(), ResultType::Duration(duration)));
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            let reader = txn.get_reader();
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = reader.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push((
                "random reads".to_string(),
                ResultType::Duration(duration),
            ));
        }

        // for _ in 0..ITERATIONS {
        //     let mut rng = make_rng();
        //     let start = Instant::now();
        //     let reader = txn.get_reader();
        //     let mut value_sum = 0;
        //     let num_scan = 10;
        //     for _ in 0..ELEMENTS {
        //         let (key, _value) = gen_pair(&mut rng);
        //         let mut iter = reader.range_from(&key);
        //         for _ in 0..num_scan {
        //             if let Some((_, value)) = iter.next() {
        //                 value_sum += value.as_ref()[0];
        //             } else {
        //                 break;
        //             }
        //         }
        //     }
        //     assert!(value_sum > 0);
        //     let end = Instant::now();
        //     let duration = end - start;
        //     println!(
        //         "{}: Random range read {} elements in {}ms",
        //         T::db_type_name(),
        //         ELEMENTS * num_scan,
        //         duration.as_millis()
        //     );
        //     results.push((
        //         "random range reads".to_string(),
        //         ResultType::Duration(duration),
        //     ));
        // }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let db2 = db.clone();
                let mut rng = rngs.pop().unwrap();
                s.spawn(move || {
                    let txn = db2.read_transaction();
                    let mut checksum = 0u64;
                    let mut expected_checksum = 0u64;
                    let reader = txn.get_reader();
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        let result = reader.get(&key).unwrap();
                        checksum += result.as_ref()[0] as u64;
                        expected_checksum += value[0] as u64;
                    }
                    assert_eq!(checksum, expected_checksum);
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random read ({} threads) {} items in {}ms",
            T::db_type_name(),
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );
        results.push((
            format!("random reads ({num_threads} threads)"),
            ResultType::Duration(duration),
        ));
    }

    let start = Instant::now();
    let deletes = ELEMENTS / 2;
    {
        let mut rng = make_rng();
        let mut txn = db.write_transaction();
        let mut inserter = txn.get_inserter();
        for i in 0..deletes {
            let (key, _value) = gen_pair(&mut rng);
            inserter.remove(&key).unwrap();
            if i % 10_000 == 0 {
                drop(inserter);
                txn.commit().unwrap();
                txn = db.write_transaction();
                inserter = txn.get_inserter();
            }
        }
        // for _ in 0..deletes {
        //     let (key, _value) = gen_pair(&mut rng);
        //     inserter.remove(&key).unwrap();
        // }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals".to_string(), ResultType::Duration(duration)));

    results
}
fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for result in walkdir::WalkDir::new(path) {
        let entry = result.unwrap();
        size += entry.metadata().unwrap().len();
    }
    size
}

#[derive(Copy, Clone, PartialEq, Debug, Eq, PartialOrd, Ord)]
enum ResultType {
    NA,
    Duration(Duration),
    SizeInBytes(u64),
}

impl std::fmt::Display for ResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use byte_unit::{Byte, UnitType};

        match self {
            ResultType::NA => write!(f, "N/A"),
            ResultType::Duration(d) => write!(f, "{d:.2?}"),
            ResultType::SizeInBytes(s) => {
                let b =
                    Byte::from_u64(*s).get_appropriate_unit(UnitType::Binary);
                write!(f, "{b:.2}")
            }
        }
    }
}

fn main() {
    let tmpdir = current_dir().unwrap().join("benchmark");
    fs::create_dir(&tmpdir).unwrap();
    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();
    #[cfg(feature = "sync")]
    let mors_results = {
        // let tmpfile =
        // tempfile::tempdir_in(&tmpdir).unwrap().path().to_path_buf();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("mors");
        let mut builder = MorsBuilder::default();
        builder.set_memtable_size(256 * 1024 * 1024);
        builder.set_dir(tmpfile.clone());
        let mors = builder.build().unwrap();
        let table = MorsBenchDatabase::new(&mors);
        println!("mors: Starting benchmark");
        let mut results = benchmark(table);
        results.push(("compaction".to_string(), ResultType::NA));
        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };
    let redb_latency_results = {
        // let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("redb");
        let mut db = redb::Database::builder()
            .set_cache_size(4 * 1024 * 1024 * 1024)
            .create(tmpfile.clone())
            .unwrap();
        let table = RedbBenchDatabase::new(&db);
        let mut results = benchmark(table);

        let start = Instant::now();
        db.compact().unwrap();
        let end = Instant::now();
        let duration = end - start;
        println!("redb: Compacted in {}ms", duration.as_millis());
        results
            .push(("compaction".to_string(), ResultType::Duration(duration)));

        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };

    let lmdb_results = {
        // let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("lmdb");
        let _ = create_dir_all(tmpfile.clone());
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(4096 * 1024 * 1024)
                .open(tmpfile.clone())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(&env);
        let mut results = benchmark(table);
        results.push(("compaction".to_string(), ResultType::NA));
        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };

    let rocksdb_results = {
        // let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("rockdb");
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(
            4 * 1_024 * 1_024 * 1_024,
        ));

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);
        let db = rocksdb::TransactionDB::open(
            &opts,
            &Default::default(),
            tmpfile.clone(),
        )
        .unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        let mut results = benchmark(table);
        results.push(("compaction".to_string(), ResultType::NA));
        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };

    let sled_results = {
        // let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("sled");
        let _ = create_dir_all(tmpfile.clone());
        let db = sled::Config::new().path(tmpfile.clone()).open().unwrap();
        let table = SledBenchDatabase::new(&db, &tmpfile);
        let mut results = benchmark(table);
        results.push(("compaction".to_string(), ResultType::NA));
        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };

    let sanakirja_results = {
        // let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let mut tmpfile = tmpdir.clone();
        tmpfile.push("sanakirja");
        // fs::remove_file(tmpfile.clone()).unwrap();
        let db = sanakirja::Env::new(tmpfile.clone(), 4096 * 1024 * 1024, 2)
            .unwrap();
        let table = SanakirjaBenchDatabase::new(&db);
        let mut results = benchmark(table);
        results.push(("compaction".to_string(), ResultType::NA));
        let size = database_size(&tmpfile);
        results.push((
            "size after bench".to_string(),
            ResultType::SizeInBytes(size),
        ));
        results
    };

    // fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    let results = [
        redb_latency_results,
        mors_results,
        lmdb_results,
        rocksdb_results,
        sled_results,
        sanakirja_results,
    ];

    let mut identified_smallests = vec![vec![false; results.len()]; rows.len()];
    for (i, identified_smallests_row) in
        identified_smallests.iter_mut().enumerate()
    {
        let mut smallest = None;
        for (j, _) in identified_smallests_row.iter().enumerate() {
            let (_, rt) = &results[j][i];
            smallest = match smallest {
                Some((_, prev)) if rt < prev => Some((j, rt)),
                Some((pi, prev)) => Some((pi, prev)),
                None => Some((j, rt)),
            };
        }
        let (j, _rt) = smallest.unwrap();
        identified_smallests_row[j] = true;
    }

    for (j, results) in results.iter().enumerate() {
        for (i, (_benchmark, result_type)) in results.iter().enumerate() {
            rows[i].push(if identified_smallests[i][j] {
                format!("**{result_type}**")
            } else {
                result_type.to_string()
            });
        }
    }

    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_width(100);
    table.set_header([
        "",
        "redb",
        "mors",
        "lmdb",
        "rocksdb",
        "sled",
        "sanakirja",
    ]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
