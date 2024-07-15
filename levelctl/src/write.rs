use std::{sync::atomic::Ordering, time::Duration};

use log::info;
use mors_traits::{kms::Kms, levelctl::LEVEL0, sstable::TableTrait};
use tokio::time::Instant;

use crate::{
    ctl::LevelCtl, error::MorsLevelCtlError, handler::LevelHandler,
    manifest::manifest_change::ManifestChange,
};
use mors_traits::kms::KmsCipher;
type Result<T> = std::result::Result<T, MorsLevelCtlError>;
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) async fn push_level0_impl(&self, table: T) -> Result<()> {
        let change = ManifestChange::new_create(
            table.id(),
            LEVEL0,
            table.cipher().map(|k| k.cipher_key_id()),
            table.compression(),
        );
        self.manifest().push_changes(vec![change]).await?;
        let handler = self.handler(LEVEL0).unwrap();
        let level0_num_tables_stall = self.level0_num_tables_stall();

        fn push_level0<T: TableTrait<K::Cipher>, K: Kms>(
            handler: &LevelHandler<T, K::Cipher>,
            table: &T,
            level0_num_tables_stall: usize,
        ) -> bool {
            let mut handler_w = handler.write();
            if handler_w.tables().len() >= level0_num_tables_stall {
                return false;
            }
            handler_w.push(table.clone());
            true
        }

        while !push_level0::<T, K>(handler, &table, level0_num_tables_stall) {
            let start = Instant::now();
            while handler.tables_len() >= level0_num_tables_stall {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let duration = start.elapsed();
            if duration.as_secs() > 1 {
                info!("Level0 stall: {} ms", duration.as_millis());
            }
            self.level0_stalls_ms()
                .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}
