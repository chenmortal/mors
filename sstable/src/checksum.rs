use serde::{Deserialize, Serialize};

use super::Result;
use crate::error::MorsTableError;
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Checksum {
    /// For storing type of Checksum algorithm used
    algo: Algorithm,
    sum: u64,
}
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub enum Algorithm {
    Crc32c,
    XxHash64,
}
impl Algorithm {
    pub(crate) fn calculate(&self, data: &[u8]) -> u64 {
        match self {
            Algorithm::Crc32c => {
                // crc32c
                crc32fast::hash(data) as u64
            }
            Algorithm::XxHash64 => {
                // xxhash
                xxhash_rust::xxh3::xxh3_64(data)
            }
        }
    }
}
impl Checksum {
    pub(crate) fn new(algo: Algorithm, data: &[u8]) -> Self {
        Checksum {
            algo,
            sum: algo.calculate(data),
        }
    }
    pub(crate) fn verify(&self, data: &[u8]) -> Result<()> {
        let sum = self.algo.calculate(data);
        if sum != self.sum {
            return Err(MorsTableError::ChecksumVerify(self.sum, sum));
        }
        Ok(())
    }
    pub(crate) fn encode_to_vec(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
}
