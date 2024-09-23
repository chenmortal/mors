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
        levels.resize_with(max_level + 1, LevelCompactStatus::default);
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

        inner_w.levels[this_level].push(plan.this_range().clone());
        inner_w.levels[next_level].push(plan.next_range().clone());
        inner_w.levels[this_level].del_size += plan.this_size() as i64;
        for t in plan.top() {
            inner_w.tables.insert(t.id());
        }
        for t in plan.bottom() {
            inner_w.tables.insert(t.id());
        }
        Ok(true)
    }
    pub(crate) fn remove<T: TableTrait<K::Cipher>, K: Kms>(
        &self,
        plan: &CompactPlan<T, K>,
    ) {
        let mut inner_w = self.0.write().unwrap();
        let this_level = plan.this_level().level();
        let next_level = plan.next_level().level();

        debug_assert!(this_level.to_usize() < inner_w.levels.len());
        debug_assert!(next_level.to_usize() < inner_w.levels.len());

        inner_w.levels[this_level.to_usize()].del_size -=
            plan.this_size() as i64;

        let this_found =
            inner_w.levels[this_level.to_usize()].remove(plan.this_range());

        let mut next_found = true;
        if this_level != next_level && !plan.next_range().is_empty() {
            next_found =
                inner_w.levels[next_level.to_usize()].remove(plan.next_range());
        }

        if !this_found || !next_found {
            let this = plan.this_range();
            let next = plan.next_range();
            log::error!(
                "Looking for: {:?} in this level {}.",
                this,
                this_level
            );
            log::error!(
                "This Level:\n{:?}",
                inner_w.levels[this_level.to_usize()]
            );
            log::error!(
                "Looking for: {:?} in next level {}.",
                next,
                next_level
            );
            log::error!(
                "Next Level:\n{:?}",
                inner_w.levels[next_level.to_usize()]
            );
        }
        for t in plan.top() {
            assert!(inner_w.tables.remove(&t.id()));
        }
        for t in plan.bottom() {
            assert!(inner_w.tables.remove(&t.id()));
        }
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyTsRange>,
    del_size: i64,
}
impl LevelCompactStatus {
    pub(crate) fn intersects(&self, target: &KeyTsRange) -> bool {
        self.ranges.iter().any(|range| range.intersects(target))
    }
    pub(crate) fn push(&mut self, ks: KeyTsRange) {
        self.ranges.push(ks);
    }
    pub(crate) fn remove(&mut self, t: &KeyTsRange) -> bool {
        match self.ranges.iter().position(|x| x == t) {
            Some(index) => {
                self.ranges.remove(index);
                true
            }
            None => false,
        }
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
#[test]
fn test_a() {
    let mut vec = vec![1, 2, 3, 4];
    match vec.iter().position(|x| *x == 2) {
        Some(i) => {
            vec.remove(i);
            true
        }
        None => false,
    };
    assert_eq!(vec, [1, 3, 4]);
    // let k = vec.retain(|&x| x != 5);
    // assert_eq!(vec, [2, 4]);
}
