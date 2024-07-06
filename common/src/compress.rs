use snap::raw::Decoder;
use thiserror::Error;
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    ZSTD(i32),
}
impl Default for CompressionType {
    fn default() -> Self {
        Self::None
    }
}
impl From<u32> for CompressionType {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Snappy,
            2 => Self::ZSTD(1),
            _ => Self::None,
        }
    }
}
impl From<CompressionType> for u32 {
    fn from(val: CompressionType) -> Self {
        match val {
            CompressionType::None => 0,
            CompressionType::Snappy => 1,
            CompressionType::ZSTD(_) => 2,
        }
    }
}
impl CompressionType {
    #[inline]
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressError> {
        match self {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Snappy => {
                Ok(snap::raw::Encoder::new().compress_vec(data)?)
            }
            CompressionType::ZSTD(level) => Ok(zstd::encode_all(data, *level)?),
        }
    }
    #[inline]
    pub fn decompress(&self, data: Vec<u8>) -> Result<Vec<u8>, CompressError> {
        match self {
            CompressionType::None => Ok(data),
            CompressionType::Snappy => {
                Ok(Decoder::new().decompress_vec(&data)?)
            }
            CompressionType::ZSTD(_) => Ok(zstd::decode_all(data.as_slice())?),
        }
    }
}
impl CompressionType {
    pub fn is_none(&self) -> bool {
        matches!(self, CompressionType::None)
    }
}
#[derive(Error, Debug)]
pub enum CompressError {
    #[error("Snappy Error: {0}")]
    SnappyError(#[from] snap::Error),
    #[error("ZSTD Error: {0}")]
    IoError(#[from] std::io::Error),
}
