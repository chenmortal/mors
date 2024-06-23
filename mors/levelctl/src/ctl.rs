use mors_traits::levelctl::{Level, LevelCtl, LevelCtlBuilder};

use crate::{
    compact::status::CompactStatus, error::MorsLevelCtlError,
    manifest::ManifestBuilder,
};

pub struct MorsLevelCtlBuilder {
    manifest: ManifestBuilder,
    max_level: Level,
}
impl Default for MorsLevelCtlBuilder {
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            max_level: 6u8.into(),
        }
    }
}
impl LevelCtlBuilder for MorsLevelCtlBuilder {
    fn build(&self) {
        let compact_status = CompactStatus::new(self.max_level.to_usize());
        
        todo!()
    }
}
pub struct MorsLevelCtl {}
impl LevelCtl for MorsLevelCtl {
    type ErrorType = MorsLevelCtlError;

    type LevelCtlBuilder = MorsLevelCtlBuilder;
}
