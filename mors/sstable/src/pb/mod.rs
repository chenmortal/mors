use proto::{checksum::Algorithm, Checksum};

use crate::error::MorsTableError;

pub mod proto;
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
        let mut checksum = Checksum::default();
        checksum.set_algo(algo);
        checksum.sum = algo.calculate(data);
        checksum
    }
    pub(crate) fn verify(&self, data: &[u8]) -> Result<(), MorsTableError> {
        let sum = self.algo().calculate(data);
        if sum != self.sum {
            return Err(MorsTableError::ChecksumVerify(self.sum, sum));
        }
        Ok(())
    }
}
