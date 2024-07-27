use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use mors_common::{file_id::SSTableId, ts::KeyTs};
use mors_traits::levelctl::Level;

use super::Result;
pub(crate) struct CompactStatus(Arc<RwLock<CompactStatusInner>>);
pub(crate) struct CompactStatusInner {
    levels: Vec<LevelCompactStatus>,
    tables: HashSet<SSTableId>,
}
impl CompactStatus {
    pub(crate) fn new(max_level: usize) -> Self {
        let mut levels = Vec::new();
        levels.resize_with(max_level, LevelCompactStatus::default);
        Self(Arc::new(RwLock::new(CompactStatusInner {
            levels,
            tables: HashSet::new(),
        })))
    }
    pub(crate) fn delete_size(&self, level: Level) -> Result<i64> {
        let inner = self.0.read()?;
        let del_size = inner.levels[level.to_usize()].del_size;
        Ok(del_size)
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyTsRange>,
    del_size: i64,
}
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyTsRange {
    left: KeyTs,
    right: KeyTs,
    inf: bool,
}
