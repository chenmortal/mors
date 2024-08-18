use std::path::PathBuf;

pub const DEFAULT_DIR: &str = "/tmp/mors";
pub const DEFAULT_VALUE_DIR: &str = "/tmp/mors";

pub trait WithDir {
    fn set_dir(&mut self, dir: PathBuf) -> &mut Self;
    fn dir(&self) -> &PathBuf;
}
pub trait WithReadOnly {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self;
    fn read_only(&self) -> bool;
}
