use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

use bytes::Bytes;
use bytesize::ByteSize;
use mors_traits::{
    kms::Kms,
    levelctl::{Level, LevelCtlTrait, LEVEL0},
    sstable::{TableBuilderTrait, TableTrait},
};

use tabled::{
    builder::Builder,
    settings::{
        object::{Cell, Columns, Rows},
        style::{BorderSpanCorrection, LineText, Offset},
        Alignment, Border, Color, Style,
    },
};

use super::Result;
use crate::ctl::LevelCtl;

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct CompactPriority {
    level: Level,
    now_total_size: usize,
    plan_delete_size: i64,
    plan_size: i64,
    target_size: usize,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Bytes>,
    target: CompactTarget,
}
pub(crate) fn fmt_compact_priorities(
    prios: &[CompactPriority],
    l0_table_len: usize,
    t_l0_table_len: usize,
) -> String {
    let mut builder = Builder::default();
    builder.push_column(vec![
        "Level",
        "NowTotalSize",
        "PlanDeleteSize",
        "PlanSize",
        "TargetSize",
        "Score",
        "Adjusted",
    ]);
    let prio_level = prios.first().unwrap().target().base_level();
    let mut base_index = 0;
    for (index, prio) in prios.iter().enumerate() {
        let mut record = Vec::with_capacity(7);
        if prio.level() == prio_level {
            base_index = index;
        }
        record.push(format!("{}", prio.level));
        let now_total_size =
            ByteSize::b(prio.now_total_size as u64).to_string_as(true);
        if prio.level == LEVEL0 {
            record.push(format!("{}({})", now_total_size, l0_table_len));
        } else {
            record.push(now_total_size);
        }

        record
            .push(ByteSize::b(prio.plan_delete_size as u64).to_string_as(true));
        record.push(ByteSize::b(prio.plan_size as u64).to_string_as(true));
        let target_size =
            ByteSize::b(prio.target_size as u64).to_string_as(true);
        if prio.level == LEVEL0 {
            record.push(format!("{}({})", target_size, t_l0_table_len));
        } else {
            record.push(target_size);
        }
        record.push(format!("{:.2}", prio.score));
        record.push(format!("{:.2}", prio.adjusted));
        builder.push_column(record);
    }
    let style = Style::modern_rounded();
    let text = "CompactPrioritys";
    let clr_green = Color::FG_GREEN;

    let table = builder
        .build()
        .with(style)
        .with(
            LineText::new(text, Rows::first()).offset(Offset::End(text.len())),
        )
        .with(Alignment::center())
        .with(BorderSpanCorrection)
        .modify(Columns::single(base_index + 1), clr_green)
        .modify(Cell::new(0, base_index + 1), Border::new().set_bottom('+'))
        .to_string();
    table
}
impl CompactPriority {
    pub(crate) fn new(level: Level, target: CompactTarget) -> Self {
        Self {
            level,
            target,
            ..Default::default()
        }
    }
    pub(crate) fn level(&self) -> Level {
        self.level
    }
    pub(crate) fn target(&self) -> &CompactTarget {
        &self.target
    }
    pub(crate) fn target_mut(&mut self) -> &mut CompactTarget {
        &mut self.target
    }
    pub(crate) fn set_target(&mut self, target: CompactTarget) {
        self.target = target;
    }
    pub(crate) fn adjusted(&self) -> f64 {
        self.adjusted
    }
    pub(crate) fn score(&self) -> f64 {
        self.score
    }
    pub(crate) fn drop_prefixes(&self) -> &[Bytes] {
        &self.drop_prefixes
    }
}
#[derive(Default, Clone, PartialEq, Eq)]
pub(crate) struct CompactTarget {
    base_level: Level,
    target_size: Vec<usize>,
    file_size: Vec<usize>,
}
impl Display for CompactTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for CompactTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = Builder::default();
        debug_assert_eq!(self.target_size.len(), self.file_size.len());
        let len = self.target_size.len();
        let mut levels = Vec::with_capacity(len + 1);
        levels.push("Level".to_string());
        for i in 0..len {
            let level: Level = i.into();
            levels.push(format!("{}", level));
        }
        builder.push_record(levels);

        let mut target_size = Vec::with_capacity(len + 1);
        target_size.push("levelTotalSize".to_string());

        target_size.extend(
            self.target_size
                .iter()
                .map(|x| ByteSize::b(*x as u64).to_string_as(true).to_string()),
        );
        builder.push_record(target_size);

        let mut file_size = Vec::with_capacity(len + 1);
        file_size.push("levelFileSize".to_string());
        file_size.extend(
            self.file_size
                .iter()
                .map(|x| ByteSize::b(*x as u64).to_string_as(true).to_string()),
        );
        builder.push_record(file_size);

        let style = Style::modern_rounded();
        let clr_green = Color::FG_GREEN;
        let text = "CompactTarget";
        let table = builder
            .build()
            .with(style)
            .with(
                LineText::new(text, Rows::first())
                    .offset(Offset::End(text.len())),
            )
            .with(Alignment::center())
            .with(BorderSpanCorrection)
            .modify(Columns::single(self.base_level.to_usize() + 1), clr_green)
            .modify(
                Cell::new(0, self.base_level.to_usize() + 1),
                Border::new().set_bottom('+'),
            )
            .to_string();
        writeln!(f, "{}", table)
    }
}
impl CompactTarget {
    pub(crate) fn target_size(&self, level: Level) -> usize {
        self.target_size[level.to_usize()]
    }
    pub(crate) fn file_size(&self, level: Level) -> usize {
        self.file_size[level.to_usize()]
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.target_size.is_empty() || self.file_size.is_empty()
    }
    pub(crate) fn set_file_size(&mut self, level: Level, size: usize) {
        self.file_size[level.to_usize()] = size;
    }
    pub(crate) fn base_level(&self) -> Level {
        self.base_level
    }
}
// levelTargets calculates the targets for levels in the LSM tree.
// The idea comes from Dynamic Level Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB.
// The sizes of levels are calculated based on the size of the lowest level, typically L6.
// So, if L6 size is 1GB, then L5 target size is 100MB, L4 target size is 10MB and so on.
//
// L0 files don't automatically go to L1. Instead, they get compacted to Lbase, where Lbase is
// chosen based on the first level which is non-empty from top (check L1 through L6). For an empty
// DB, that would be L6.  So, L0 compactions go to L6, then L5, L4 and so on.
//
// Lbase is advanced to the upper levels when its target size exceeds BaseLevelSize. For
// example, when L6 reaches 1.1GB, then L4 target sizes becomes 11MB, thus exceeding the
// BaseLevelSize of 10MB. L3 would then become the new Lbase, with a target size of 1MB <
// BaseLevelSize.
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn target(&self) -> CompactTarget {
        let mut target = CompactTarget {
            base_level: LEVEL0,
            target_size: vec![0; self.max_level().to_usize() + 1],
            file_size: vec![0; self.max_level().to_usize() + 1],
        };

        let max_handler = self.handler(self.max_level()).unwrap();

        let mut level_size = max_handler.total_size();
        let base_level_size = self.config().base_level_total_size();

        for i in (1..=self.max_level().into()).rev() {
            // L6->L1, if level size <= base level size and base_level not changed, then Lbase = i
            if level_size <= base_level_size && target.base_level == LEVEL0 {
                target.base_level = i.into();
            }
            // target size >= base level size
            let target_size = level_size.max(base_level_size);
            target.target_size[i] = target_size;
            // if L6 size is 1GB, then L5 target size is 100MB, L4 target size is 10MB and so on.
            level_size /= self.config().level_size_multiplier();
        }
        target.target_size[LEVEL0.to_usize()] =
            self.config().level0_tables_len()
                * self.config().level0_table_size();

        let mut table_size = self.table_builder().table_size();
        for i in 0..=self.max_level().to_usize() {
            if i == 0 {
                // level0_size == memtable_size
                target.file_size[i] = self.config().level0_table_size();
            } else if i <= target.base_level.to_usize() {
                target.file_size[i] = table_size;
            } else {
                table_size *= self.config().table_size_multiplier();
                target.file_size[i] = table_size;
            }
        }

        // Bring the base level down to the last empty level.
        for level in
            (target.base_level.to_usize() + 1)..self.max_level().to_usize()
        {
            if self.handler(level.into()).unwrap().total_size() > 0 {
                break;
            };
            target.base_level = level.into();
        }

        let base_level = target.base_level;

        // If the base level is empty and the next level size is less than the
        // target size, pick the next level as the base level.
        if base_level < self.max_level()
            && self.handler(base_level).unwrap().total_size() == 0
            && self.handler(base_level + 1).unwrap().total_size()
                < target.target_size[base_level.to_usize() + 1]
        {
            target.base_level = base_level + 1;
        }
        target
    }
    pub(crate) fn pick_compact_levels(&self) -> Result<Vec<CompactPriority>> {
        let mut prios = Vec::with_capacity(self.handlers_len());
        let target = self.target();

        for level in 0..=self.max_level().to_usize() {
            let level = level.into();
            let plan_delete_size = self.compact_status().delete_size(level)?;
            let now_total_size = self.handler(level).unwrap().total_size();
            let plan_size = now_total_size as i64 - plan_delete_size;
            let target_size = target.target_size(level);
            let score = plan_size as f64 / target_size as f64;
            let adjusted = score;
            let priority = CompactPriority {
                level,
                score,
                adjusted,
                drop_prefixes: vec![],
                target: target.clone(),
                now_total_size,
                plan_delete_size,
                plan_size,
                target_size,
            };
            prios.push(priority);
        }
        let l0_tables_len = self.handler(LEVEL0).unwrap().tables_len();
        let level0_score =
            l0_tables_len as f64 / self.config().level0_tables_len() as f64;
        prios[0].score = level0_score;
        prios[0].adjusted = level0_score;

        assert_eq!(prios.len(), self.handlers_len());

        // The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
        // If Li-1 has score > 1.0, then we'll divide Li-1 score by Li.
        // If Li score is >= 1.0, then Li-1 score is reduced,
        // which means we'll prioritize the compaction of lower levels (L5, L4 and so on) over the higher levels (L0, L1 and so on).
        // On the other hand, if Li score is < 1.0, then we'll increase the priority of Li-1.
        // Overall what this means is, if the bottom level is already overflowing, then de-prioritize
        // compaction of the above level. If the bottom level is not full, then increase the priority of above level.
        let mut pre_level = 0;
        for level in target.base_level.to_usize()..=self.max_level().to_usize()
        {
            if prios[pre_level].adjusted >= 1.0 {
                // Avoid absurdly large scores by placing a floor on the score that we'll
                // adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
                const MIN_SCORE: f64 = 0.01;
                if prios[level].score >= MIN_SCORE {
                    prios[pre_level].adjusted /= prios[level].adjusted;
                } else {
                    prios[pre_level].adjusted /= MIN_SCORE;
                }
            }
            pre_level = level;
        }

        // descend sort the levels by their adjusted score.
        prios.sort_by(|a, b| {
            b.adjusted
                .partial_cmp(&a.adjusted)
                .unwrap_or(Ordering::Greater)
        });

        Ok(prios)
    }
}
#[cfg(test)]
mod tests {

    use bytesize::ByteSize;

    use crate::compaction::priority::fmt_compact_priorities;

    use super::{CompactPriority, CompactTarget};

    #[test]
    fn test_compact_target_display() {
        let compact = CompactTarget {
            base_level: 1u32.into(),
            target_size: vec![
                ByteSize::mib(1).as_u64() as usize,
                ByteSize::mib(2).as_u64() as usize,
                ByteSize::mib(3).as_u64() as usize,
            ],
            file_size: vec![
                ByteSize::mib(1).as_u64() as usize,
                ByteSize::mib(2).as_u64() as usize,
                ByteSize::mib(3).as_u64() as usize,
            ],
        };
        println!("{}", compact);
    }
    #[test]
    fn test_compact_priority() {
        let target = CompactTarget {
            base_level: 1u32.into(),
            ..Default::default()
        };
        let priority1 = CompactPriority {
            level: 0u32.into(),
            now_total_size: ByteSize::mib(1).as_u64() as usize,
            plan_delete_size: ByteSize::kib(256).as_u64() as i64,
            plan_size: ByteSize::mib(1).as_u64() as i64,
            target_size: ByteSize::mib(2).as_u64() as usize,
            score: 0.75,
            adjusted: 0.75,
            target: target.clone(),
            ..Default::default()
        };
        let priority2 = CompactPriority {
            level: 1u32.into(),
            now_total_size: ByteSize::mib(1).as_u64() as usize,
            plan_delete_size: ByteSize::kib(256).as_u64() as i64,
            plan_size: ByteSize::mib(1).as_u64() as i64,
            target_size: ByteSize::mib(2).as_u64() as usize,
            score: 0.9,
            adjusted: 0.8,
            target: target.clone(),
            ..Default::default()
        };
        let priority3 = CompactPriority {
            level: 2u32.into(),
            now_total_size: ByteSize::mib(1).as_u64() as usize,
            plan_delete_size: ByteSize::kib(256).as_u64() as i64,
            plan_size: ByteSize::mib(1).as_u64() as i64,
            target_size: ByteSize::mib(2).as_u64() as usize,
            score: 0.8,
            adjusted: 0.7,
            target: target.clone(),
            ..Default::default()
        };
        let prios = vec![priority1, priority2, priority3];
        let priors = fmt_compact_priorities(&prios, 2, 3);
        // let prioris = CompactPrioritys { prios: &prios };
        println!("{}", priors);
    }
}
