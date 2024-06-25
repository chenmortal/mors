pub trait Table: Sized {
    type ErrorType;
    type TableBuilder: TableBuilder;
}
pub trait TableBuilder: Default {
    
}
pub trait Block {
    
}
pub trait TableIndexBuf:Sized {
    
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BlockIndex(u32);
impl From<u32> for BlockIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<BlockIndex> for u32 {
    fn from(val: BlockIndex) -> Self {
        val.0
    }
}
impl From<usize> for BlockIndex {
    fn from(value: usize) -> Self {
        Self(value as u32)
    }
}
impl From<BlockIndex> for usize {
    fn from(val: BlockIndex) -> Self {
        val.0 as usize
    }
}