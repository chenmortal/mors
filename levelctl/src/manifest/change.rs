use super::Result;
use mors_common::compress::CompressionType;
use mors_common::file_id::SSTableId;
use mors_traits::kms::CipherKeyId;
use mors_traits::levelctl::Level;
use serde::{Deserialize, Serialize};
pub struct ManifestChangeSet {
    pub changes: Vec<ManifestChange>,
}
impl ManifestChangeSet {
    pub fn add(&mut self, change: ManifestChange) {
        self.changes.push(change);
    }
    pub fn encode_to_vec(&self) -> Vec<u8> {
        bincode::serialize(&self.changes).unwrap()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            changes: bincode::deserialize(bytes)?,
        })
    }
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ManifestChange {
    Create {
        id: SSTableId,
        level: Level,
        cipher_key_id: Option<CipherKeyId>,
        encryption_algo: Option<EncryptionAlgo>,
        compression: CompressionType,
    },
    Delete {
        id: SSTableId,
        level: Level,
    },
}
impl ManifestChange {
    pub fn new_create(
        table_id: SSTableId,
        level: Level,
        cipher_key_id: Option<CipherKeyId>,
        compression: CompressionType,
    ) -> Self {
        Self::Create {
            id: table_id,
            level,
            cipher_key_id,
            encryption_algo: Some(EncryptionAlgo::Aes),
            compression,
        }
    }
    pub fn new_delete(table_id: SSTableId, level: Level) -> Self {
        Self::Delete {
            id: table_id,
            level,
        }
    }
    pub fn table_id(&self) -> SSTableId {
        match self {
            Self::Create { id, .. } => *id,
            Self::Delete { id, .. } => *id,
        }
    }
    pub fn encode_to_vec(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}
#[cfg(test)]
mod tests {
    use mors_common::compress::CompressionType;
    use mors_traits::levelctl::LEVEL0;

    use crate::manifest::change::ManifestChange;

    #[test]
    fn test_manifest() {
        let create = ManifestChange::new_create(
            1.into(),
            LEVEL0,
            None,
            CompressionType::Snappy,
        );
        let delete = ManifestChange::new_delete(1.into(), LEVEL0);
        let create_vec = create.encode_to_vec();
        let delete_vec = delete.encode_to_vec();
        dbg!(create_vec);
        dbg!(delete_vec);
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub enum EncryptionAlgo {
    Aes = 0,
}
