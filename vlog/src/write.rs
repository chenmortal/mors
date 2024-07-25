use std::slice::IterMut;

use mors_common::{
    kv::{Entry, Meta, ValuePointer},
    page_size,
};
use mors_traits::{kms::Kms, vlog::VlogCtlTrait};

use crate::vlogctl::VlogCtl;
use crate::Result;

impl<K: Kms> VlogCtl<K> {
    pub(crate) async fn write_impl<'a>(
        &self,
        iter_mut: Vec<IterMut<'a, (Entry, ValuePointer)>>,
    ) -> Result<()> {
        let mut buf = Vec::with_capacity(page_size());
        let mut latest = self.latest_logfile()?;

        let threshold_sender = self.threshold().sender();
        for iter in iter_mut {
            let mut value_sizes = Vec::with_capacity(iter.len());
            {
                let mut latest_w = latest.write()?;
                for (entry, vp) in iter {
                    buf.clear();
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
                    let len = latest_w.encode_entry(&mut buf, entry, offset)?;

                    entry.set_meta(tmp_meta);
                    *vp = ValuePointer::new(
                        latest_w.id(),
                        len as u32,
                        offset as u64,
                    );

                    if !buf.is_empty() {
                        let start = self.woffset_fetch_add(buf.len());
                        let end = start + buf.len();
                        if end >= latest_w.max_size() {
                            latest_w.set_len(end)?;
                        }
                        latest_w.write_all(&buf[..len])?;
                    }
                }
            }
            threshold_sender.send(value_sizes).await?;
            if self.woffset() > self.vlog_file_size() {
                let mut latest_w = latest.write()?;
                latest_w.flush()?;
                latest_w.set_len(self.woffset())?;
                let new = self.create_new()?;
                drop(latest_w);
                latest = new;
            }
        }

        if self.woffset() > self.vlog_file_size() {
            let mut latest_w = latest.write()?;
            latest_w.flush()?;
            latest_w.set_len(self.woffset())?;
            let _ = self.create_new()?;
        }

        Ok(())
    }
}
