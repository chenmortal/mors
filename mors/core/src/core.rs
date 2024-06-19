use mors_common::lock::DBLockGuard;

pub struct Mors{
    pub(crate) core: Core,
}

pub struct Core{
    pub(crate) lock_guard: DBLockGuard,
}

pub struct DBCoreBuilder{

}
