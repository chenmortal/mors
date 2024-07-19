use std::{cmp::Ordering, mem::size_of};

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
pub fn search<F>(n: usize, f: F) -> Result<usize, usize>
where
    F: Fn(usize) -> Ordering,
{
    let mut left = 0;
    let mut right = n;
    while left < right {
        let mid = (left + right) >> 1;
        let ord = f(mid);
        if ord == Ordering::Greater {
            right = mid;
        } else if ord == Ordering::Less {
            left = mid + 1;
        } else {
            return Ok(mid);
        }
    }
    Err(left)
}
#[test]
fn test_search() {
    assert_eq!(search(5, |n| { n.cmp(&2) }), Ok(2));
    assert_eq!(search(10, |n| { n.cmp(&11) }), Err(10));
    assert_eq!(search(5, |n| { (n as isize).cmp(&-1) }), Err(0));
    let v = [1, 3, 5, 7, 9];
    assert_eq!(search(3, |n| { v[n].cmp(&2) }), Err(1));
    assert_eq!(search(3, |n| { v[n].cmp(&3) }), Ok(1));
    assert_eq!(search(3, |n| { v[n].cmp(&5) }), Ok(2));
    assert_eq!(search(3, |n| { v[n].cmp(&6) }), Err(3));
}
