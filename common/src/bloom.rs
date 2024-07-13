use std::ops::Deref;

use bytes::Bytes;

//copy from LevelDB-Go
pub struct Bloom(Bytes);
impl Deref for Bloom {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub struct BloomBorrow<'a>(&'a [u8]);
impl<'a> From<&'a [u8]> for BloomBorrow<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}
impl<'a> BloomBorrow<'a> {
    pub fn may_contain_key(&self, bytes: &[u8]) -> bool {
        self.may_contain(Bloom::hash(bytes))
    }
    #[inline]
    pub fn may_contain(&self, mut hash: u32) -> bool {
        let filter = self.0;
        if filter.len() < 2 {
            return false;
        }
        let k = filter[filter.len() - 1];
        let bit_len = (filter.len() - 1) * 8;
        let delta = hash.rotate_left(15);
        for _ in 0..k {
            let bit_pos = hash as usize % bit_len;
            if filter[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            hash = hash.wrapping_add(delta);
        }
        true
    }
}
impl Bloom {
    pub fn new(key_hashes: &[u32], false_positive_rate: f64) -> Self {
        let bits_per_key = Self::bits_per_key(false_positive_rate);
        let k = (-1.0 * false_positive_rate.ln()).clamp(1.0, 30.0) as u32;
        let mut bit_len = (key_hashes.len() * bits_per_key as usize).max(64);
        let byte_len = (bit_len as f64 / 8.0).ceil() as usize;
        bit_len = byte_len * 8;

        let want = byte_len + 1;
        let mut cap = 1024;
        while cap < want {
            cap += cap / 4;
        }
        let mut filter = Vec::with_capacity(cap);
        filter.resize_with(want, || 0u8);

        for hash in key_hashes {
            let mut hash = *hash;
            let delta = hash.rotate_left(15);
            (0..k).for_each(|_| {
                let bit_pos = hash as usize % bit_len;
                filter[bit_pos / 8] |= 1 << (bit_pos % 8);
                hash = hash.wrapping_add(delta);
            });
        }
        filter[byte_len] = k as u8;
        Self(filter.into())
    }
    #[inline]
    pub fn may_contain_key(&self, bytes: &[u8]) -> bool {
        BloomBorrow(self).may_contain(Self::hash(bytes))
    }

    #[inline]
    pub fn may_contain(&self, hash: u32) -> bool {
        BloomBorrow(self).may_contain(hash)
    }
    #[inline]
    pub(crate) fn bits_per_key(false_positive_rate: f64) -> u32 {
        (-1.0 * false_positive_rate.log2()).ceil().max(0.0) as u32
    }

    pub fn hash(mut bytes: &[u8]) -> u32 {
        const SEED: u32 = 0xbc9f1d34;
        const M: u32 = 0xc6a4a793;
        let mut hash = SEED ^ (bytes.len() as u32).wrapping_mul(M);
        while bytes.len() >= 4 {
            hash = hash.wrapping_add(
                bytes[0] as u32
                    | (bytes[1] as u32) << 8
                    | (bytes[2] as u32) << 16
                    | (bytes[3] as u32) << 24,
            );
            hash = hash.wrapping_mul(M);
            hash ^= hash >> 16;
            bytes = &bytes[4..];
        }
        let len = bytes.len();
        if len == 3 {
            hash = hash.wrapping_add(
                (bytes[0] as u32)
                    | (bytes[1] as u32) << 8
                    | (bytes[2] as u32) << 16,
            )
        } else if len == 2 {
            hash = hash.wrapping_add((bytes[0] as u32) | (bytes[1] as u32) << 8)
        } else if len == 1 {
            hash = hash.wrapping_add(bytes[0] as u32);
        };
        if len != 0 {
            hash = hash.wrapping_mul(M);
            hash ^= hash >> 24;
        }
        hash
    }
}
#[cfg(test)]
mod tests {
    use super::Bloom;

    #[test]
    fn test_hash() {
        assert_eq!(Bloom::hash("".as_ref()), 0xbc9f1d34);
        assert_eq!(Bloom::hash("a".as_ref()), 0x286e9db0);
        assert_eq!(Bloom::hash("ab".as_ref()), 0x39aca330);
        assert_eq!(Bloom::hash("abc".as_ref()), 0x855d012f);
        assert_eq!(Bloom::hash("abcd".as_ref()), 0xb9c83353);
        assert_eq!(Bloom::hash("abcde".as_ref()), 0x41d2c26d);
        assert_eq!(Bloom::hash("abcdefghi".as_ref()), 0xf065ec74);
        assert_eq!(Bloom::hash("你好".as_ref()), 0x6466387);
    }

    #[test]
    fn test_bits_per_key() {
        assert_eq!(Bloom::bits_per_key(0.01), 7);
        assert_eq!(Bloom::bits_per_key(0.001), 10);
        assert_eq!(Bloom::bits_per_key(0.0001), 14);
        assert_eq!(Bloom::bits_per_key(0.00001), 17);
        dbg!(Bloom::bits_per_key(0.002));
    }
    #[test]
    fn test_bloom_filter() {
        let next_length = |x: usize| {
            if x < 10 {
                return x + 1;
            }
            if x < 100 {
                return x + 10;
            }
            if x < 1000 {
                return x + 100;
            }
            x + 1000
        };
        let mut medicore_filters = 0;
        let mut good_filters = 0;
        let mut length = 1;
        while length <= 10_000 {
            let mut keys = Vec::with_capacity(length);
            for i in 0..length as u32 {
                keys.push(i.to_le_bytes());
            }
            let mut hashes = Vec::new();
            for key in keys.iter() {
                hashes.push(Bloom::hash(key.as_ref()));
            }
            let bloom = Bloom::new(&hashes, 0.001);
            assert!(bloom.0.len() < length * 10 / 8 + 40);
            for key in keys.iter() {
                assert!(bloom.may_contain_key(key.as_ref()));
            }
            let mut false_positive = 0f64;
            for i in 0..10_000 {
                if bloom
                    .may_contain_key((1e9 as u32 + i).to_le_bytes().as_ref())
                {
                    false_positive += 1.0;
                };
            }
            assert!(
                false_positive <= 0.02 * 10_000.0,
                "length={}: {} false positive in 10_000",
                length,
                false_positive
            );
            if false_positive > 0.0125 * 10_000.0 {
                medicore_filters += 1;
            } else {
                good_filters += 1;
            }
            length = next_length(length);
        }
        assert!(
            medicore_filters < good_filters / 5,
            "{} mediocre filters but only {} good filters",
            medicore_filters,
            good_filters
        )
    }
}
