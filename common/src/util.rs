use std::mem::size_of;

use bytes::{Buf, BufMut};

pub trait BufExt: Buf {
    fn get_vec_u32(&mut self) -> Vec<u32> {
        const SIZE: usize = size_of::<u32>();
        let capacity = self.chunk().len() / SIZE;
        let mut v = Vec::<u32>::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(self.get_u32());
        }
        v
    }
}
pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}
impl BufExt for &[u8] {}
impl Encode for Vec<u32> {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::<u8>::with_capacity(self.len() + 4);
        for t in self.iter() {
            result.put_u32(*t);
        }
        result
    }
}
