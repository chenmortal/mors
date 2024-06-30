use std::mem::size_of;

use bytes::Buf;

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
impl BufExt for &[u8] {}
