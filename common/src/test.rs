use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};

pub fn get_rng(seed: &str) -> StdRng {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    let result = hasher.finalize();
    let seed = result.into();
    StdRng::from_seed(seed)
}
pub fn gen_random_entry(rng: StdRng) {

    // Entry::new();
    
}
