use std::path::PathBuf;

use mors_traits::{
    default::{WithDir, WithReadOnly},
    kms::Kms,
    vlog::{DiscardTrait, VlogCtlBuilderTrait, VlogCtlTrait, VlogError},
};

use crate::error::MorsVlogError;

pub struct VlogCtl {}
impl<K: Kms, D: DiscardTrait> VlogCtlTrait<K, D> for VlogCtl {
    type ErrorType = MorsVlogError;

    type LevelCtlBuilder = VlogCtlBuilder<K>;
}
#[derive(Debug)]
pub struct VlogCtlBuilder<K: Kms> {
    _kms: K,
}
impl<K: Kms, D: DiscardTrait> VlogCtlBuilderTrait<VlogCtl, K, D>
    for VlogCtlBuilder<K>
{
    async fn build(&self, kms: K) -> Result<VlogCtl, VlogError> {
        todo!()
    }
}
impl<K: Kms> WithDir for VlogCtlBuilder<K> {
    fn set_dir(&mut self, dir: PathBuf) -> &mut Self {
        todo!()
    }

    fn dir(&self) -> &PathBuf {
        todo!()
    }
}
impl<K: Kms> WithReadOnly for VlogCtlBuilder<K> {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        todo!()
    }

    fn read_only(&self) -> bool {
        todo!()
    }
}
