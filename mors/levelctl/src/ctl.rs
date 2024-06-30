use std::{
    marker::PhantomData,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::info;
use mors_common::closer::{CloseNotify, Throttle};
use mors_traits::{
    cache::Cache,
    default::DEFAULT_DIR,
    file_id::{FileId, SSTableId},
    kms::{Kms, KmsCipher},
    levelctl::{Level, LevelCtl, LevelCtlBuilder},
    sstable::{BlockTrait, TableIndexBufTrait, TableTrait},
};

use tokio::select;

use crate::{
    compact::status::CompactStatus,
    error::MorsLevelCtlError,
    manifest::{Manifest, ManifestBuilder},
};
pub struct MorsLevelCtl<
    T: TableTrait<C, B, TB, K>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: KmsCipher,
> {
    table: T,
    c: PhantomData<C>,
    b: PhantomData<B>,
    tb: PhantomData<TB>,
    k: PhantomData<K>,
}
impl<
        T: TableTrait<C, B, TB, K>,
        C: Cache<B, TB>,
        B: BlockTrait,
        TB: TableIndexBufTrait,
        K: KmsCipher,
    > LevelCtl<T, C, B, TB, K> for MorsLevelCtl<T, C, B, TB, K>
{
    type ErrorType = MorsLevelCtlError;

    type LevelCtlBuilder = MorsLevelCtlBuilder<T, C, B, TB, K>;
}
type Result<T> = std::result::Result<T, MorsLevelCtlError>;
pub struct MorsLevelCtlBuilder<
    T: TableTrait<C, B, TB, K>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: KmsCipher,
> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    max_level: Level,
    dir: PathBuf,
}
impl<
        T: TableTrait<C, B, TB, K>,
        C: Cache<B, TB>,
        B: BlockTrait,
        TB: TableIndexBufTrait,
        K: KmsCipher,
    > Default for MorsLevelCtlBuilder<T, C, B, TB, K>
{
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
            max_level: 6_u8.into(),
            dir: PathBuf::from(DEFAULT_DIR),
        }
    }
}
impl<
        T: TableTrait<C, B, TB, K>,
        C: Cache<B, TB>,
        B: BlockTrait,
        TB: TableIndexBufTrait,
        K: KmsCipher,
    > LevelCtlBuilder<MorsLevelCtl<T, C, B, TB, K>, T, C, B, TB, K>
    for MorsLevelCtlBuilder<T, C, B, TB, K>
{
    async fn build(&self, kms: impl Kms) -> Result<()> {
        let compact_status = CompactStatus::new(self.max_level.to_usize());
        let manifest = self.manifest.build()?;

        self.open_tables_by_manifest(&manifest, kms).await?;

        Ok(())
    }
}
impl<
        T: TableTrait<C, B, TB,K>,
        C: Cache<B, TB>,
        B: BlockTrait,
        TB: TableIndexBufTrait,
        K: KmsCipher,
    > MorsLevelCtlBuilder<T, C, B, TB,K>
{
    async fn open_tables_by_manifest(
        &self,
        manifest: &Manifest,
        kms: impl Kms,
    ) -> Result<()> {
        manifest.revert(&self.dir)?;

        let num_opened = Arc::new(AtomicUsize::new(0));
        // let table_len = manifest.table_len();
        let manifest_lock = manifest.lock();
        let tables = manifest_lock.tables();

        let watch_close_notify =
            Self::watch_num_opened(num_opened.clone(), tables.len());

        let mut max_id: SSTableId = 0.into();
        let mut throttle = Throttle::<MorsLevelCtlError>::new(3);

        // let mut tasks=vec![Vec::new();self.max_level.to_usize()];

        for (id, table) in tables.iter() {
            let opened = num_opened.clone();
            let path = id.join_dir(&self.dir);
            let permit = throttle.acquire().await?;

            max_id = max_id.max(*id);

            let compress = table.compress();
            let cipher_id = table.key_id();
        }

        Ok(())
    }
    fn watch_num_opened(
        num_opened: Arc<AtomicUsize>,
        table_len: usize,
    ) -> CloseNotify {
        use tokio::time::interval;
        use tokio::time::Instant;

        let start = Instant::now();
        let close = CloseNotify::new();
        let close_clone = close.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(3));
            loop {
                select! {
                    i = tick.tick() => {
                        info!("{} tables opened out of {} in {} ms",
                            num_opened.load(Ordering::Relaxed),
                            table_len,
                            i.duration_since(start).as_millis(),
                        )
                    }
                    _ = close_clone.wait() => {
                        info!("All {} tables opened in {} ms",
                            num_opened.load(Ordering::Relaxed),
                            start.elapsed().as_millis());
                        break;
                    }
                }
            }
        });
        close
    }
}
