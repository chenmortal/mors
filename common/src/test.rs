use rand::{rngs::StdRng, Rng, SeedableRng};
use sha2::{Digest, Sha256};

use crate::kv::Entry;

pub fn get_rng(seed: &str) -> StdRng {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    let result = hasher.finalize();
    let seed = result.into();
    StdRng::from_seed(seed)
}
pub fn generate_random_fixed_len(rng: &mut StdRng, length: u8) -> Vec<u8> {
    let mut bytes = vec![0u8; length as usize];
    rng.fill(&mut bytes[..]);
    bytes
}
pub fn generate_random_bytes(
    rng: &mut StdRng,
    max_size: Option<usize>,
) -> Vec<u8> {
    let length: u8 = rng.gen();
    if let Some(s) = max_size {
        let length = length % (s as u8);
        // length = if length == 0 { 10 } else { length };
        let length = length.max(2);
        return generate_random_fixed_len(rng, length);
    }
    generate_random_fixed_len(rng, length)
}
pub fn gen_random_entry(rng: &mut StdRng, max_size: Option<usize>) -> Entry {
    let key = generate_random_bytes(rng, max_size);
    let value = generate_random_bytes(rng, max_size);
    let mut entry = Entry::new(key.into(), value.into());
    let txn: u64 = rng.gen();
    entry.set_version(txn.into());
    entry
}
pub fn gen_random_entries(
    rng: &mut StdRng,
    count: usize,
    fix_size: Option<usize>,
    max_size: Option<usize>,
) -> Vec<Entry> {
    let mut entries = Vec::new();
    for _ in 0..count {
        if let Some(s) = fix_size {
            let key = generate_random_fixed_len(rng, s as u8);
            let value = generate_random_fixed_len(rng, s as u8);
            let mut entry = Entry::new(key.into(), value.into());
            let txn: u64 = rng.gen();
            entry.set_version(txn.into());
            entries.push(entry);
        } else {
            entries.push(gen_random_entry(rng, max_size));
        }
    }
    entries
}
pub fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

