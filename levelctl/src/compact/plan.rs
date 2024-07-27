use mors_traits::{kms::Kms, levelctl::LEVEL0, sstable::TableTrait};

use crate::{ctl::LevelCtl, handler::LevelHandler};

use super::priority::CompactPriority;

pub(crate) struct CompactPlan<T: TableTrait<K::Cipher>, K: Kms> {
    task_id: usize,
    priority: CompactPriority,
    this_level: LevelHandler<T, K::Cipher>,
    next_level: LevelHandler<T, K::Cipher>,
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    fn gen_plan(
        &self,
        task_id: usize,
        priority: CompactPriority,
    ) -> CompactPlan<T, K> {
        let this_level = self.handler(priority.level()).unwrap().clone();

        if priority.level() == LEVEL0 {
            let next_level =
                self.handler(self.target().base_level()).unwrap().clone();
            let plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
            };
            plan
        } else {
            let next_level = this_level.to_owned();
            let plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
            };
            plan
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
    fn fill_tables_l0() {

    }
    fn fill_tables_l0_to_lbase(){

    }
    fn fill_
}
