use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    fs::read_dir,
    hash::Hash,
    path::{Path, PathBuf},
};

use thiserror::Error;
#[derive(Error, Debug)]
pub enum FildIdError {
    #[error("failed parse PathBuf {path:?} , maybe not ends with {suffix}")]
    ParseError { path: PathBuf, suffix: &'static str },
}
pub trait FileId: From<u32> + Into<u32> + Debug + Copy + Eq + Hash {
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
    fn parse_set_from_dir<P: AsRef<Path>>(dir: P) -> HashSet<Self> {
        let mut id_set = HashSet::new();
        let dir = dir.as_ref();
        if let Ok(read_dir) = read_dir(dir) {
            for dir_entry in read_dir.flatten() {
                let path = dir_entry.path();
                if path.is_file() {
                    if let Ok(id) = Self::parse(path) {
                        id_set.insert(id);
                    };
                }
            }
        };
        id_set
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
impl Display for MemtableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}.mem", self.0)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SSTableId(u32);
impl From<u32> for SSTableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<SSTableId> for u32 {
    fn from(val: SSTableId) -> Self {
        val.0
    }
}
impl FileId for SSTableId {
    const SUFFIX: &'static str = ".sst";
}
impl Display for SSTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}.sst", self.0)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VlogId(u32);
impl From<u32> for VlogId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<VlogId> for u32 {
    fn from(val: VlogId) -> Self {
        val.0
    }
}
impl FileId for VlogId {
    const SUFFIX: &'static str = ".vlog";
}
impl Display for VlogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}.vlog", self.0)
    }
}
