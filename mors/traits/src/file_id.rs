use std::{
    collections::HashSet, fmt::{Debug, Display}, fs::read_dir, path::{Path, PathBuf}
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SSTableId(u32);
impl From<u32> for SSTableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for SSTableId {
    fn into(self) -> u32 {
        self.0
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
impl SSTableId {
    pub fn parse_set_from_dir<P: AsRef<Path>>(dir: P) -> HashSet<SSTableId> {
        let mut id_set = HashSet::new();
        let dir = dir.as_ref();
        if let Ok(read_dir) = read_dir(dir) {
            for ele in read_dir {
                if let Ok(entry) = ele {
                    let path = entry.path();
                    if path.is_file() {
                        if let Ok(id) = Self::parse(path) {
                            id_set.insert(id);
                        };
                    }
                }
            }
        };
        return id_set;
    }
}