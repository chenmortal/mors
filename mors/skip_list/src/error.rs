use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsSkipListError {
    #[error(transparent)]
    ArenaError(#[from] ArenaError),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Null Pointer Error")]
    NullPointerError,
}
#[derive(Error, Debug)]
pub enum ArenaError {
    #[error(transparent)]
    LayoutError(#[from] std::alloc::LayoutError),
    #[error("Arena too small, toWrite:{to_write}, newTotal:{new_total}, limit:{limit}")]
    SizeTooSmall {
        to_write: usize,
        new_total: usize,
        limit: usize,
    },
    #[error("Null Pointer Error")]
    NullPointerError,
    #[error("Offset {offset} + data size {size} out of bound {limit}")]
    OffsetOutOfBound {
        offset: usize,
        size: usize,
        limit: usize,
    },
    #[error("Zero length error")]
    ZeroLengthError,
}
