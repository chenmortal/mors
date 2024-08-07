use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use log::{debug, info};
use mors_common::file_id::{FileId, SSTableId};
use mors_common::kv::{Meta, ValueMeta, ValuePointer};
use mors_common::ts::KeyTs;
use mors_traits::default::WithDir;
use mors_traits::iter::{
    CacheIterator, KvCacheIter, KvCacheIterator, KvCacheMergeIterator,
    KvSeekIter,
};
use mors_traits::kms::KmsCipher;
use mors_traits::levelctl::{Level, LevelCtlTrait, LEVEL0};
use mors_traits::sstable::{
    CacheTableConcatIter, SSTableError, TableBuilderTrait, TableWriterTrait,
};
use mors_traits::vlog::DiscardTrait;
use mors_traits::{kms::Kms, sstable::TableTrait};
use tokio::task::JoinHandle;

use crate::manifest::manifest_change::ManifestChange;
use crate::manifest::Manifest;
use crate::{ctl::LevelCtl, error::MorsLevelCtlError};

use super::plan::{CompactPlan, CompactPlanReadGuard, KeyTsRange};
use super::{CompactContext, Result};

impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) async fn compact<D: DiscardTrait>(
        &self,
        task_id: usize,
        level: Level,
        plan: &mut CompactPlan<T, K>,
        context: CompactContext<T, K, D>,
    ) -> Result<()> {
        let priority = plan.priority();
        let target = priority.target();

        if target.is_empty() {
            return Err(MorsLevelCtlError::EmptyCompactTarget);
        };

        let now = SystemTime::now();
        let this_level = plan.this_level();
        let next_level = plan.next_level();

        debug_assert!(plan.splits().is_empty());

        if this_level.level() != next_level.level() {
            plan.add_splits();
        }

        let new_tables =
            self.compact_build_tables(level, plan, &context).await?;

        self.do_manifest_change(&new_tables, plan, context.manifest())
            .await?;

        let new_tables_size =
            new_tables.iter().fold(0, |acc, x| acc + x.size());
        let old_tables_size =
            plan.top().iter().fold(0, |acc, x| acc + x.size())
                + plan.bottom().iter().fold(0, |acc, x| acc + x.size());

        plan.next_level().replace(plan.bottom(), &new_tables);
        plan.this_level().delete(plan.top());

        let table_to_string = |tables: &[T]| {
            let mut v = Vec::with_capacity(tables.len());
            tables.iter().for_each(|t| {
                v.push(format!("{:>5}", Into::<u32>::into(t.id())))
            });
            v.join(".")
        };

        let duration = now.elapsed().unwrap();
        if duration.as_secs() > 2 {
            info!(
                "[{task_id}] LOG Compact {this_level}-{next_level} \
                took {duration} ms with {new_tables_size} bytes written, {old_tables_size} bytes read, \
                new tables: {new_tables}, \
                old tables: \
                --   top:   {top_tables} \
                --   bottom:{bottom_tables}",
                task_id = task_id,
                this_level = plan.this_level().level(),
                next_level = plan.next_level().level(),
                duration = duration.as_millis(),
                new_tables = table_to_string(&new_tables),
                top_tables = table_to_string(plan.top()),
                bottom_tables = table_to_string(plan.bottom()),
                new_tables_size = new_tables_size,
                old_tables_size = old_tables_size,
            );
        }

        if plan.this_level().level() != LEVEL0
            && new_tables.len() > 2 * self.config().level_size_multiplier()
        {
            info!(
                "This Range (num tables: {top_len}) \
                -- Left:{top_left:?} \
                -- Right:{top_right:?} \
                Next Range (num tables: {bottom_len}) \
                -- Left:{bottom_left:?} \
                -- Right:{bottom_right:?}",
                top_len = plan.top().len(),
                top_left = plan.this_range().left(),
                top_right = plan.this_range().right(),
                bottom_len = plan.bottom().len(),
                bottom_left = plan.next_range().left(),
                bottom_right = plan.next_range().right(),
            )
        }
        Ok(())
    }
    async fn do_manifest_change(
        &self,
        new_tables: &Vec<T>,
        plan: &mut CompactPlan<T, K>,
        manifest: &Manifest,
    ) -> Result<()> {
        let mut changes = Vec::with_capacity(
            new_tables.len() + plan.top().len() + plan.bottom().len(),
        );
        for table in new_tables {
            let cipher = table.cipher().map(|c| c.cipher_key_id());
            changes.push(ManifestChange::new_create(
                table.id(),
                plan.next_level().level(),
                cipher,
                table.compression(),
            ));
        }

        for table in plan.top() {
            changes.push(ManifestChange::new_delete(table.id()));
        }
        for table in plan.bottom() {
            changes.push(ManifestChange::new_delete(table.id()));
        }

        manifest.push_changes(changes).await?;
        Ok(())
    }
    // compactBuildTables merges topTables and botTables to form a list of new tables.
    pub(crate) async fn compact_build_tables<D: DiscardTrait>(
        &self,
        level: Level,
        plan: &mut CompactPlan<T, K>,
        context: &CompactContext<T, K, D>,
    ) -> Result<Vec<T>> {
        let top = plan.top();
        let bottom = plan.bottom();
        debug!(
            "Top tables count: {} Bottom tables count {}",
            top.len(),
            bottom.len()
        );

        let valid = bottom
            .iter()
            .filter(|t| {
                !plan.priority().drop_prefixes().iter().any(|prefix| {
                    t.smallest().key().starts_with(prefix)
                        && t.biggest().key().starts_with(prefix)
                })
            })
            .cloned()
            .collect::<Vec<_>>();

        let new_iter = || {
            let mut out: Vec<Box<dyn KvCacheIterator<ValueMeta>>> = Vec::new();
            if level == LEVEL0 {
                for t in top.iter().rev() {
                    out.push(Box::new(t.iter(false)));
                }
            } else if !top.is_empty() {
                assert_eq!(top.len(), 1);
                out = vec![Box::new(top[0].iter(false))]
            };
            out.push(Box::new(CacheTableConcatIter::new(valid.clone(), false)));
            out
        };
        let mut compact_task = Vec::new();
        let plan_clone = Arc::new(plan.clone());
        for kr in plan.splits() {
            let iters = new_iter();
            if let Some(merge) = KvCacheMergeIterator::new(iters) {
                compact_task.push(tokio::spawn(self.clone().sub_compact(
                    merge,
                    kr.clone(),
                    plan_clone.clone(),
                    context.clone(),
                )));
            };
        }

        let mut tables = Vec::new();
        for compact in compact_task {
            for table_task in compact.await?? {
                if let Some(t) = table_task.await?? {
                    tables.push(t);
                }
            }
        }

        tables.sort_by(|a, b| a.biggest().cmp(b.biggest()));
        Ok(tables)
    }
    async fn sub_compact<D: DiscardTrait>(
        self,
        mut merge_iter: KvCacheMergeIterator,
        kr: KeyTsRange,
        plan: Arc<CompactPlan<T, K>>,
        context: CompactContext<T, K, D>,
    ) -> Result<Vec<JoinHandle<std::result::Result<Option<T>, SSTableError>>>>
    {
        let mut all_tables = plan.top().to_vec();
        all_tables.extend_from_slice(plan.bottom());

        let is_intersect =
            self.check_intersect(&all_tables, plan.next_level().level());

        let target = plan.priority().target();

        if kr.left().is_empty() {
            merge_iter.next()?;
        } else {
            let left = kr.left().encode();
            let left_borrow = left.as_slice();
            merge_iter.seek(left_borrow.into())?;
        }

        let mut discard_stats = HashMap::new();
        let mut table_task = Vec::new();
        while merge_iter.valid() {
            if !kr.right().is_empty()
                && merge_iter.key().unwrap() == *kr.right()
            {
                break;
            }
            let mut builder = self.table_builder().clone();
            let target_size = target.file_size(plan.next_level().level());
            builder.set_table_size(target_size);
            let cipher = context.kms.latest_cipher()?;
            let writer = T::new_writer(builder.clone(), cipher.clone());

            let mut context = AddKeyContext {
                last_key: Default::default(),
                skip_key: Default::default(),
                num_versions: Default::default(),
                discard_stats: &mut discard_stats,
                first_key_has_discard_set: Default::default(),
                ctl: &self,
                kr: &kr,
                is_intersect,
                writer,
                plan: &plan,
            };
            context.push(&mut merge_iter)?;
            let next_id: SSTableId =
                self.next_id().fetch_add(1, Ordering::AcqRel).into();

            let path = next_id.join_dir(self.table_builder().dir());
            let mut writer = context.writer;
            table_task.push(tokio::spawn(async move {
                writer.flush_to_disk(path).await?;
                builder.open(next_id, cipher).await
            }));
        }
        for (id, discard) in discard_stats.iter() {
            context.discard().update(*id as u64, *discard as i64)?;
        }
        debug!("Discard stats updated {:?}", discard_stats);
        Ok(table_task)
    }
    fn check_intersect(&self, tables: &[T], level: Level) -> bool {
        let kr = KeyTsRange::from_slice::<T, K>(tables);
        for level in level.to_usize()..=self.max_level().to_usize() {
            let handler = self.handler(level.into()).unwrap();
            let guard = CompactPlanReadGuard {
                this_level: handler.read(),
                next_level: handler.read(),
            };
            let range = guard.this_level.table_index_by_range(&guard, &kr);
            if range.count() > 0 {
                return true;
            }
        }
        false
    }
}

struct AddKeyContext<'a, T: TableTrait<K::Cipher>, K: Kms> {
    last_key: KeyTs,
    skip_key: KeyTs,
    num_versions: usize,
    discard_stats: &'a mut HashMap<u32, u64>,
    first_key_has_discard_set: bool,
    ctl: &'a LevelCtl<T, K>,
    plan: &'a CompactPlan<T, K>,
    kr: &'a KeyTsRange,
    is_intersect: bool,
    writer: T::TableWriter,
}
impl<'a, T: TableTrait<K::Cipher>, K: Kms> AddKeyContext<'a, T, K> {
    fn push(&mut self, iter: &mut KvCacheMergeIterator) -> Result<()> {
        let start = SystemTime::now();
        let mut num_keys = 0;
        let mut num_skips = 0;
        let mut range_check = 0;
        let mut table_key_range = KeyTsRange::default();

        while iter.valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            if self.plan.drop_prefixes().iter().any(|p| key.starts_with(p)) {
                num_keys += 1;
                self.update_discard(&value);
                iter.next()?;
                continue;
            };

            if !self.skip_key.is_empty() {
                if key.key() == self.skip_key.key() {
                    num_skips += 1;
                    self.update_discard(&value);
                    iter.next()?;
                    continue;
                }
                self.skip_key = Default::default();
            }

            if key.key() != self.last_key.key() {
                self.first_key_has_discard_set = false;
                if !self.kr.right().is_empty()
                    && iter.key().unwrap() == *self.kr.right()
                {
                    break;
                }
                if self.writer.reached_capacity() {
                    break;
                }
                self.last_key = key.into();
                self.num_versions = 0;
                self.first_key_has_discard_set =
                    value.meta().contains(Meta::DISCARD_EARLIER_VERSIONS);
                if table_key_range.left().is_empty() {
                    table_key_range.set_left(key.into());
                }

                table_key_range.set_right(self.last_key.clone());
                range_check += 1;

                if range_check % 5000 == 0 {
                    let exceeds_allowed_overlap = {
                        let level = self.plan.next_level().level() + 1;
                        if level.to_u8() <= 1 || level > self.ctl.max_level() {
                            false
                        } else {
                            let handler = self.ctl.handler(level).unwrap();
                            // let table = handler.read();;
                            let lock = CompactPlanReadGuard {
                                this_level: handler.read(),
                                next_level: handler.read(),
                            };
                            let range = lock
                                .this_level
                                .table_index_by_range(&lock, self.kr);
                            range.count() >= 10
                        }
                    };
                    if exceeds_allowed_overlap {
                        break;
                    }
                }
            }

            let is_delete = value.is_deleted_or_expired();
            if !value.meta().contains(Meta::MERGE_ENTRY) {
                self.num_versions += 1;
                let last_valid_version =
                    value.meta().contains(Meta::DISCARD_EARLIER_VERSIONS)
                        || self.num_versions
                            == self.ctl.config().num_versions_to_keep();

                if is_delete || last_valid_version {
                    self.skip_key = key.into();

                    if (is_delete || !last_valid_version) && !self.is_intersect
                    {
                        num_skips += 1;
                        self.update_discard(&value);
                        iter.next()?;
                        continue;
                    }
                }
            }
            num_keys += 1;

            let mut vptr_len = None;
            if value.meta().contains(Meta::VALUE_POINTER) {
                vptr_len =
                    Some(ValuePointer::decode(value.value()).unwrap().size());
            }
            if self.first_key_has_discard_set || is_delete {
                self.writer.push_stale(&key, &value, vptr_len);
            } else {
                self.writer.push(&key, &value, vptr_len);
            }
        }
        debug!(
            "Pushed {} keys, skipped {} keys, took {:?}",
            num_keys,
            num_skips,
            start.elapsed().unwrap()
        );
        Ok(())
    }
    fn update_discard(&mut self, value: &ValueMeta) {
        if value.meta().contains(Meta::VALUE_POINTER) {
            let vp = ValuePointer::decode(value.value()).unwrap();
            match self.discard_stats.get_mut(&vp.fid()) {
                Some(v) => {
                    *v += vp.size() as u64;
                }
                None => {
                    self.discard_stats.insert(vp.fid(), vp.size() as u64);
                }
            };
        }
    }
}
