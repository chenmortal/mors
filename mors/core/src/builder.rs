use std::path::PathBuf;

use mors_common::lock::DBLockGuardBuilder;

use crate::core::{Core, Mors};
use crate::Result;
pub struct MorsBuilder {
    read_only: bool,
    dir: PathBuf,
}
impl MorsBuilder {
    pub fn build(&self) -> Result<Mors> {
        let mut guard_builder = DBLockGuardBuilder::new();

        guard_builder.add_dir(self.dir.clone());
        guard_builder.read_only(self.read_only);

        let lock_guard = guard_builder.build()?;

        Ok(Mors {
            core: Core { lock_guard },
        })
    }
}
