use std::slice::IterMut;

use mors_common::kv::{Entry, Meta, ValuePointer};
use mors_traits::file::StorageTrait;
use mors_traits::{kms::Kms, vlog::VlogCtlTrait};
use mors_wal::error::MorsWalError;

use crate::vlogctl::VlogCtl;
use crate::Result;

impl<K: Kms, S: StorageTrait> VlogCtl<K, S> {
    pub(crate) async fn write_impl<'a>(
        &self,
        iter_mut: Vec<IterMut<'a, (Entry, ValuePointer)>>,
    ) -> Result<()> {
        // let mut buf = Vec::with_capacity(page_size());
        let mut latest = self.latest_logfile()?;

        let threshold_sender = self.threshold().sender();
        for iter in iter_mut {
            let mut value_sizes = Vec::with_capacity(iter.len());
            {
                // let mut latest_w = latest.write()?;
                for (entry, vp) in iter {
                    // buf.clear();
                    value_sizes.push(entry.value().len());
                    entry.set_value_threshold(self.value_threshold());
                    if entry.value().len() < entry.value_threshold() {
                        *vp = ValuePointer::default();
                        continue;
                    }
                    let offset = self.woffset();
                    let tmp_meta = entry.meta();
                    entry.meta_mut().remove(Meta::TXN);
                    entry.meta_mut().remove(Meta::FIN_TXN);

                    let size = match latest.append_entry(entry) {
                        Ok(size) => size,
                        Err(MorsWalError::StorageFull) => {
                            latest = self.create_new()?;
                            latest.append_entry(entry)?
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    };
                    self.woffset_fetch_add(size);

                    entry.set_meta(tmp_meta);
                    *vp = ValuePointer::new(
                        latest.id(),
                        size as u32,
                        offset as u64,
                    );
                }
            }
            threshold_sender.send(value_sizes).await?;
            let len = self.woffset();
            if len >= self.vlog_file_size() {
                latest.flush()?;
                let new = self.create_new()?;
                latest = new;
            }
        }
        let len = self.woffset();
        if len >= self.vlog_file_size() {
            latest.flush()?;
            let _ = self.create_new()?;
        }

        Ok(())
    }
}
