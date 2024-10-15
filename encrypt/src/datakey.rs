use super::Result;
use mors_common::ts::PhyTs;
use mors_traits::kms::CipherKeyId;
use serde::{Deserialize, Serialize};
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct DataKey {
    /// this is table's id
    pub key_id: CipherKeyId,
    /// this is other encryption key
    pub data: Vec<u8>,
    /// just for decrypt or  encrypt DataKey.data with Config.encryptionkey
    pub iv: Vec<u8>,
    pub created_at: PhyTs,
}
impl DataKey {
    pub(crate) fn encode_to_vec(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
}
