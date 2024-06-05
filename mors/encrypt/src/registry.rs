use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;

use mors_traits::ts::PhyTs;

use crate::id::CipherKeyId;
use crate::pb::encryption::DataKey;

#[derive(Debug, Default)]
pub(crate) struct KeyRegistryInner {
    data_keys: HashMap<CipherKeyId, DataKey>,
    last_created: PhyTs, //last_created is the timestamp(seconds) of the last data key,
    next_key_id: CipherKeyId,
    fp: Option<File>,
    // cipher: Option<AesCipher>,
    data_key_rotation_duration: Duration,
}
