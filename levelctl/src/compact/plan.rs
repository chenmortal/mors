use std::ops::RangeInclusive;

use mors_common::ts::KeyTs;
use mors_traits::{kms::Kms, levelctl::LEVEL0, sstable::TableTrait};
use parking_lot::RwLockReadGuard;

use crate::{
    ctl::LevelCtl,
    handler::{LevelHandler, LevelHandlerTables},
};

use super::priority::CompactPriority;
use super::Result;

pub(crate) struct CompactPlan<T: TableTrait<K::Cipher>, K: Kms> {
    task_id: usize,
    priority: CompactPriority,
    this_level: LevelHandler<T, K>,
    next_level: LevelHandler<T, K>,
    top: Vec<T>,
    bottom: Vec<T>,
    this_range: KeyTsRange,
    next_range: KeyTsRange,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for CompactPlan<T, K> {
    fn default() -> Self {
        Self {
            task_id: Default::default(),
            priority: Default::default(),
            this_level: Default::default(),
            next_level: Default::default(),
            top: Default::default(),
            bottom: Default::default(),
            this_range: Default::default(),
            next_range: Default::default(),
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> CompactPlan<T, K> {
    pub(crate) fn this_level(&self) -> &LevelHandler<T, K> {
        &self.this_level
    }
    pub(crate) fn next_level(&self) -> &LevelHandler<T, K> {
        &self.next_level
    }
    pub(crate) fn this_range(&self) -> &KeyTsRange {
        &self.this_range
    }
    pub(crate) fn next_range(&self) -> &KeyTsRange {
        &self.next_range
    }
    pub(crate) fn top(&self) -> &[T] {
        &self.top
    }
    pub(crate) fn bottom(&self) -> &[T] {
        &self.bottom
    }
}
pub(crate) struct CompactPlanReadGuard<'a, T: TableTrait<K::Cipher>, K: Kms> {
    this_level: RwLockReadGuard<'a, LevelHandlerTables<T, K>>,
    next_level: RwLockReadGuard<'a, LevelHandlerTables<T, K>>,
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn gen_plan(
        &self,
        task_id: usize,
        priority: CompactPriority,
    ) -> Result<CompactPlan<T, K>> {
        let this_level = self.handler(priority.level()).unwrap().clone();

        if priority.level() == LEVEL0 {
            let next_level =
                self.handler(self.target().base_level()).unwrap().clone();
            let mut plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
                ..Default::default()
            };
            self.fill_tables_l0(&mut plan)?;
            Ok(plan)
        } else {
            let next_level = this_level.to_owned();
            let plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
                ..Default::default()
            };
            Ok(plan)
        }
    }
    // fillTablesL0 would try to fill tables from L0 to be compacted with Lbase.
    // If it can not do that, it would try to compact tables from L0 -> L0.
    // Say L0 has 10 tables.
    // fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
    // Next call to fill_level0_tables would run L0ToLbase again, which fails this time.
    // So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
    // be compacted within L0. Additionally, it would set the compaction range in
    // cstatus to inf, so no other L0 -> Lbase compactions can happen.
    fn fill_tables_l0(&self, plan: &mut CompactPlan<T, K>) -> Result<bool> {
        Ok(self.fill_tables_l0_to_lbase(plan)? || self.fill_tables_l0_to_l0())
    }
    fn fill_tables_l0_to_lbase(
        &self,
        plan: &mut CompactPlan<T, K>,
    ) -> Result<bool> {
        if *plan.next_level.level() == LEVEL0 {
            unreachable!("Base level can't be zero")
        }

        if (0.0..1.0).contains(&plan.priority.adjusted()) {
            return Ok(false);
        }
        let lock = CompactPlanReadGuard::<T, K> {
            this_level: self.handler(*plan.this_level.level()).unwrap().read(),
            next_level: self.handler(*plan.next_level.level()).unwrap().read(),
        };

        let top = lock.this_level.tables().to_vec();
        if top.is_empty() {
            return Ok(false);
        }

        if !plan.priority.drop_prefixes().is_empty() {
            // Use all tables if drop prefix is set. We don't want to compact only a
            // sub-range. We want to compact all the tables.
            plan.this_range =
                KeyTsRange::from_slice::<T, K>(lock.this_level.tables());
            plan.top = lock.this_level.tables().to_vec();
        } else {
            plan.top.clear();
            let mut s = KeyTsRange::from::<T, K>(&top[0]);
            for t in top.iter().skip(1) {
                let other = KeyTsRange::from::<T, K>(t);
                if s.intersects(&other) {
                    plan.top.push(t.clone());
                    s.extend(other);
                } else {
                    break;
                }
            }
            plan.this_range = s;
        }
        let index_range = lock
            .next_level
            .table_index_by_range(&lock, &plan.this_range);
        plan.bottom = lock.next_level.tables()[index_range].to_vec();

        if plan.bottom.is_empty() {
            plan.next_range = plan.this_range.clone();
        } else {
            plan.next_range = KeyTsRange::from_slice::<T, K>(&plan.bottom);
        }

        self.compact_status().check_update(&lock, plan)
    }
    fn fill_tables_l0_to_l0(&self) -> bool {
        false
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyTsRange {
    left: KeyTs,
    right: KeyTs,
    inf: bool,
}

impl KeyTsRange {
    pub(crate) fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty() && !self.inf
    }
    pub(crate) fn intersects(&self, other: &Self) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }
        if self.inf || other.inf {
            return true;
        }
        !(self.right < other.left || self.left > other.right)
    }
    pub(crate) fn extend(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        if self.left.is_empty() || other.left < self.left {
            self.left = other.left;
        }
        if self.right.is_empty() || self.right < other.right {
            self.right = other.right;
        }
        if other.inf {
            self.inf = true;
        }
    }
    pub(crate) fn from_slice<T: TableTrait<K::Cipher>, K: Kms>(
        value: &[T],
    ) -> Self {
        if value.is_empty() {
            return Self::default();
        }
        let (smallest, biggest) = value.iter().fold(
            (value[0].smallest(), value[0].biggest()),
            |(smallest, biggest), table| {
                (smallest.min(table.smallest()), biggest.max(table.biggest()))
            },
        );
        Self {
            left: KeyTs::new(smallest.key().clone(), u64::MAX.into()),
            right: KeyTs::new(biggest.key().clone(), 0.into()),
            inf: false,
        }
    }
    pub(crate) fn from<T: TableTrait<K::Cipher>, K: Kms>(value: &T) -> Self {
        Self {
            left: KeyTs::new(value.smallest().key().clone(), u64::MAX.into()),
            right: KeyTs::new(value.biggest().key().clone(), 0.into()),
            inf: false,
        }
    }
    pub(crate) fn inf() -> Self {
        Self {
            left: KeyTs::default(),
            right: KeyTs::default(),
            inf: true,
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandlerTables<T, K> {
    fn table_index_by_range(
        &self,
        _lock: &CompactPlanReadGuard<T, K>,
        kr: &KeyTsRange,
    ) -> RangeInclusive<usize> {
        if kr.left.is_empty() || kr.right.is_empty() {
            return 0..=0;
        }
        let left_index = self
            .tables()
            .binary_search_by(|t| t.biggest().cmp(&kr.left))
            .unwrap_or_else(|i| i);
        let right_index = self
            .tables()
            .binary_search_by(|t| t.smallest().cmp(&kr.right))
            .unwrap_or_else(|i| i); // if t.smallest==kr.right, so need this table.
        left_index..=right_index
    }
}
