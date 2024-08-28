use core::{Core, CoreBuilder};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use error::MorsError;
use mors_encrypt::{cipher::AesCipher, registry::MorsKms};
use mors_levelctl::ctl::LevelCtl;
use mors_memtable::memtable::Memtable;

use mors_skip_list::skip_list::SkipList;
use mors_sstable::table::Table;

use mors_vlog::vlogctl::VlogCtl;
use tokio::runtime::Builder;
pub mod core;
mod error;
mod flush;
mod read;
mod test;
mod txn;
mod write;
pub type Result<T> = std::result::Result<T, MorsError>;

type MorsMemtable = Memtable<SkipList, MorsKms>;
type MorsLevelCtl = LevelCtl<Table<AesCipher>, MorsKms>;
type MorsTable = Table<AesCipher>;
type MorsLevelCtlType = LevelCtl<MorsTable, MorsKms>;
type MorsVlog = VlogCtl<MorsKms>;
#[derive(Clone)]
pub struct Mors {
    #[cfg(feature = "sync")]
    inner: Arc<MorsInner>,
    #[cfg(not(feature = "sync"))]
    inner: MorsInner,
}
struct MorsInner {
    core: Core<
        MorsMemtable,
        MorsKms,
        MorsLevelCtl,
        Table<AesCipher>,
        SkipList,
        MorsVlog,

    >,
    #[cfg(feature = "sync")]
    runtime: tokio::runtime::Runtime,
}

pub struct MorsBuilder {
    builder: CoreBuilder<
        MorsMemtable,
        MorsKms,
        MorsLevelCtlType,
        MorsTable,
        SkipList,

        MorsVlog,
    >,
    #[cfg(feature = "sync")]
    tokio_builder: tokio::runtime::Builder,
}

#[allow(clippy::derivable_impls)]
impl Default for MorsBuilder {
    fn default() -> Self {
        let mut tokio_builder = Builder::new_multi_thread();
        tokio_builder.enable_all();
        Self {
            builder: Default::default(),
            #[cfg(feature = "sync")]
            tokio_builder,
        }
    }
}
impl Deref for Mors {
    type Target = Core<
        Memtable<SkipList, MorsKms>,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,
        MorsVlog,

    >;

    fn deref(&self) -> &Self::Target {
        &self.inner.core
    }
}
impl Deref for MorsBuilder {
    type Target = CoreBuilder<
        Memtable<SkipList, MorsKms>,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,

        MorsVlog,
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
    #[cfg(feature = "sync")]
    pub fn tokio_builder(&mut self) -> &mut Builder {
        &mut self.tokio_builder
    }

    #[cfg(feature = "sync")]
    pub fn build(&mut self) -> Result<Mors> {
        let runtime = self.tokio_builder.build()?;
        let k = runtime.block_on(self.builder.build())?;
        let inner = MorsInner { core: k, runtime };
        Ok(Mors {
            inner: Arc::new(inner),
        })
    }
    #[cfg(not(feature = "sync"))]
    pub async fn build(&mut self) -> Result<Mors> {
        let core = self.builder.build().await?;
        let inner = MorsInner { core };
        Ok(Mors {
            inner: Arc::new(inner),
        })
    }
}
