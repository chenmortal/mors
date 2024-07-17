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
pub fn generate_random_bytes(rng: &mut StdRng) -> Vec<u8> {
    let length: u8 = rng.gen();
    generate_random_fixed_len(rng, length)
}
pub fn gen_random_entry(rng: &mut StdRng) -> Entry {
    let key = generate_random_bytes(rng);
    let value = generate_random_bytes(rng);
    let mut entry = Entry::new(key.into(), value.into());
    let txn: u64 = rng.gen();
    entry.set_version(txn.into());
    entry
}
pub fn gen_random_entries(rng: &mut StdRng, count: usize) -> Vec<Entry> {
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push(gen_random_entry(rng));
    }
    entries
}
