use core::{Core, CoreBuilder};

use error::MorsError;
use mors_encrypt::{cipher::AesCipher, registry::MorsKms};
use mors_levelctl::ctl::LevelCtl;
use mors_memtable::memtable::Memtable;

use mors_skip_list::impls::MorsSkipList;
use mors_sstable::table::Table;
use mors_txn::manager::TxnManager;
pub mod core;
mod error;
mod test;

pub type Result<T> = std::result::Result<T, MorsError>;
pub type Mors = Core<
    Memtable<MorsSkipList, MorsKms>,
    MorsKms,
    LevelCtl<Table<MorsKms>, MorsKms>,
    Table<MorsKms>,
>;
pub type MorsBuilder = CoreBuilder<
    Memtable<MorsSkipList, MorsKms>,
    MorsKms,
    LevelCtl<Table<AesCipher>, MorsKms>,
    Table<AesCipher>,
    TxnManager,
>;

// #[cfg(test)]
// mod tests {
// use crate::{Mors, MorsBuilder};

fn test_build() {
    // Mors::b
    // MorsBuilder::default();
    MorsBuilder::default();
}
// }
