use mors_traits::{
    kms::Kms,
    levelctl::{Level, LevelCtl, LevelCtlBuilder},
    sstable::Table,
};

use crate::{
    compact::status::CompactStatus,
    error::MorsLevelCtlError,
    manifest::{Manifest, ManifestBuilder},
};
pub struct MorsLevelCtl<T: Table> {
    table: T,
}
impl<T: Table> LevelCtl<T> for MorsLevelCtl<T> {
    type ErrorType = MorsLevelCtlError;

    type LevelCtlBuilder = MorsLevelCtlBuilder<T>;
}
type Result<T> = std::result::Result<T, MorsLevelCtlError>;
pub struct MorsLevelCtlBuilder<T: Table> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    max_level: Level,
}
impl<T: Table> Default for MorsLevelCtlBuilder<T> {
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
            max_level: 6u8.into(),
        }
    }
}
impl<T: Table> LevelCtlBuilder<MorsLevelCtl<T>, T> for MorsLevelCtlBuilder<T> {
    fn build(&self, kms: impl Kms) -> Result<()> {
        let compact_status = CompactStatus::new(self.max_level.to_usize());
        let manifest = self.manifest.build()?;
        Ok(())
    }
}
impl<T: Table> MorsLevelCtlBuilder<T> {
    fn open_tables_by_manifest(&self, manifest: &Manifest, kms: impl Kms) {
        let manifest_lock = manifest.lock();
    }
}
