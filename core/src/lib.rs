use core::{Core, CoreBuilder};
use std::ops::{Deref, DerefMut};

use error::MorsError;
use mors_encrypt::{cipher::AesCipher, registry::MorsKms};
use mors_levelctl::ctl::LevelCtl;
use mors_memtable::memtable::Memtable;

use mors_skip_list::skip_list::SkipList;
use mors_sstable::table::Table;
use mors_txn::manager::TxnManager;
pub mod core;
mod error;
mod flush;
mod read;
mod test;
mod write;
pub type Result<T> = std::result::Result<T, MorsError>;

type MorsMemtable = Memtable<SkipList, MorsKms>;
type MorsLevelCtl = LevelCtl<Table<AesCipher>, MorsKms>;
type MorsTable = Table<AesCipher>;
type MorsLevelCtlType = LevelCtl<MorsTable, MorsKms>;
pub struct Mors {
    core: Core<MorsMemtable, MorsKms, MorsLevelCtl, Table<AesCipher>, SkipList>,
}

#[derive(Default)]
pub struct MorsBuilder {
    builder: CoreBuilder<
        MorsMemtable,
        MorsKms,
        MorsLevelCtlType,
        MorsTable,
        SkipList,
        TxnManager,
    >,
}
impl Deref for Mors {
    type Target = Core<
        Memtable<SkipList, MorsKms>,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,
    >;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
impl Deref for MorsBuilder {
    type Target = CoreBuilder<
        Memtable<SkipList, MorsKms>,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,
        TxnManager,
    >;

    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}
impl DerefMut for MorsBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.builder
    }
}
impl MorsBuilder {
    pub async fn build(&mut self) -> Result<Mors> {
        let core = self.builder.build().await?;
        Ok(Mors { core })
    }
}
