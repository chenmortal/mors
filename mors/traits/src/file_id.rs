use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};

use thiserror::Error;
#[derive(Error, Debug)]
pub enum FildIdError {
    #[error("failed parse PathBuf {path:?} , maybe not ends with {suffix}")]
    ParseError { path: PathBuf, suffix: &'static str },
}
pub trait FileId: From<u32> + Into<u32> + Debug + Copy {
    const SUFFIX: &'static str;

    fn parse<P: AsRef<Path>>(path: P) -> Result<Self, FildIdError>
    where
        Self: Sized,
    {
        let path_buf = path.as_ref();
        if let Some(name) = path_buf.file_name() {
            if let Some(name) = name.to_str() {
                if name.ends_with(Self::SUFFIX) {
                    let name = name.trim_end_matches(Self::SUFFIX);
                    if let Ok(id) = name.parse::<u32>() {
                        return Ok(id.into());
                    };
                };
            }
        };

        Err(FildIdError::ParseError {
            path: path_buf.to_owned(),
            suffix: Self::SUFFIX,
        })
    }
    fn join_dir<P: AsRef<Path>>(self, parent_dir: P) -> PathBuf {
        let dir = parent_dir.as_ref();
        let id: u32 = self.into();
        dir.join(format!("{:06}{}", id, Self::SUFFIX))
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemtableId(u32);
impl From<u32> for MemtableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<MemtableId> for u32 {
    fn from(value: MemtableId) -> Self {
        value.0
    }
}
impl FileId for MemtableId {
    const SUFFIX: &'static str = ".mem";
}

