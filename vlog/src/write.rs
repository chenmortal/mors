use std::slice::IterMut;

use mors_common::kv::{Entry, ValuePointer};
use mors_traits::kms::Kms;

use crate::{error::MorsVlogError, vlogctl::VlogCtl};
type Result<T> = std::result::Result<T, MorsVlogError>;
impl<K: Kms> VlogCtl<K> {
    fn write(
        &self,
        iter_mut: Vec<IterMut<(Entry, ValuePointer)>>,
    ) -> Result<()> {
        // let mut buf = Vec::with_capacity(page_size());
        let latest = self.latest_logfile()?;
        for iter in iter_mut {
            let latest_w = latest.write()?;

            for (entry, vp) in iter {
                // let entry_size = entry.size();
                // let vp_size = vp.size();
                // if buf.len() + entry_size + vp_size > page_size() {
                //     latest.write(&buf)?;
                //     buf.clear();
                // }
                // entry.write(&mut buf)?;
                // vp.write(&mut buf)?;
            }
        }
        Ok(())
    }
}
