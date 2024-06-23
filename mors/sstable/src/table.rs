use mors_common::compress::CompressionType;

use crate::pb::proto::checksum;

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
#[derive(Debug, Clone, Copy)]
pub enum ChecksumVerificationMode {
    // NoVerification indicates DB should not verify checksum for SSTable blocks.
    NoVerification,

    // OnTableRead indicates checksum should be verified while opening SSTtable.
    OnTableRead,

    // OnBlockRead indicates checksum should be verified on every SSTable block read.
    OnBlockRead,

    // OnTableAndBlockRead indicates checksum should be verified
    // on SSTable opening and on every block read.
    OnTableAndBlockRead,
}
impl Default for ChecksumVerificationMode {
    fn default() -> Self {
        Self::NoVerification
    }
}

pub struct MorsTableBuilder {
    table_size: usize,
    table_capacity: usize,
    // ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
    checksum_verify_mode: ChecksumVerificationMode,
    checksum_algo: checksum::Algorithm,
    // BloomFalsePositive is the false positive probabiltiy of bloom filter.
    bloom_false_positive: f64,

    // BlockSize is the size of each block inside SSTable in bytes.
    block_size: usize,

    // Compression indicates the compression algorithm used for block compression.
    compression: CompressionType,

    zstd_compression_level: i32,
}
impl Default for MorsTableBuilder {
    fn default() -> Self {
        Self {
            table_size: 2 << 20,
            table_capacity: ((2 << 20) as f64 * 0.95) as usize,
            checksum_verify_mode: ChecksumVerificationMode::default(),
            checksum_algo: checksum::Algorithm::Crc32c,
            bloom_false_positive: 0.01,
            block_size: 4 * 1024,
            compression: CompressionType::default(),
            zstd_compression_level: 1,
        }
    }
}

