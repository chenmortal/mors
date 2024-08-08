use std::{
    collections::HashSet,
    ops::Deref,
    sync::{Arc, RwLock},
};

use mors_common::file_id::SSTableId;
use mors_traits::{kms::Kms, levelctl::Level, sstable::TableTrait};

use super::{
    plan::{CompactPlan, CompactPlanReadGuard, KeyTsRange},
    Result,
};
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
    pub(crate) fn intersects(
        &self,
        level: Level,
        target: &KeyTsRange,
    ) -> Result<bool> {
        let inner = self.0.read()?;
        Ok(inner.levels[level.to_usize()].intersects(target))
    }
}
impl Deref for CompactStatus {
    type Target = Arc<RwLock<CompactStatusInner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyTsRange>,
    del_size: i64,
}
impl CompactStatus {
    pub(crate) fn check_update<T: TableTrait<K::Cipher>, K: Kms>(
        &self,
        _lock: &CompactPlanReadGuard<T, K>,
        plan: &CompactPlan<T, K>,
    ) -> Result<bool> {
        let mut inner_w = self.0.write()?;
        let this_level = plan.this_level().level().to_usize();
        let next_level = plan.next_level().level().to_usize();

        debug_assert!(this_level < inner_w.levels.len());
        debug_assert!(next_level < inner_w.levels.len());

        if inner_w.levels[this_level].intersects(plan.this_range())
            || inner_w.levels[next_level].intersects(plan.next_range())
        {
            return Ok(false);
        }

        inner_w.levels[this_level]
            .ranges
            .push(plan.this_range().clone());
        inner_w.levels[next_level]
            .ranges
            .push(plan.next_range().clone());

        for t in plan.top() {
            inner_w.tables.insert(t.id());
        }
        for t in plan.bottom() {
            inner_w.tables.insert(t.id());
        }
        Ok(true)
    }
}
impl LevelCompactStatus {
    pub(crate) fn intersects(&self, target: &KeyTsRange) -> bool {
        self.ranges.iter().any(|range| range.intersects(target))
    }
    pub(crate) fn ranges_mut(&mut self) -> &mut Vec<KeyTsRange> {
        &mut self.ranges
    }
}
impl CompactStatusInner {
    pub(crate) fn tables(&self) -> &HashSet<SSTableId> {
        &self.tables
    }
    pub(crate) fn tables_mut(&mut self) -> &mut HashSet<SSTableId> {
        &mut self.tables
    }
    pub(crate) fn levels_mut(&mut self) -> &mut [LevelCompactStatus] {
        &mut self.levels
    }
}
