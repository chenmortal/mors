extern crate thiserror;

use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use rand::Rng;

use arena::Arena;
use error::MorsSkipListError;

const SKL_MAX_HEIGHT: usize = 20; //<20 !=20

pub mod arena;
mod error;
mod impls;
#[cfg(test)]
mod test;
type Result<T> = std::result::Result<T, MorsSkipListError>;

///0 <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->  
///1 <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->  
///2 <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->  
///3 <head> ----------> [2] --------------------------------------------------> [9] ---------->  
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}
pub struct SkipList {
    ///the height of the highest node in the list
    height: AtomicUsize,
    ///the head of the list
    head: NonNull<Node>,
    ///the memory pool of the list
    arena: Arena,
    ///the compare function of the list
    cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering,
}
impl SkipList {
    pub fn new(max_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Result<Self>
    where
        Self: Sized,
    {
        let arena = Arena::new(max_size)?;
        arena.alloc(0u8)?;
        let head: &mut Node = arena.alloc_with(Node::default)?;
        head.set_height(SKL_MAX_HEIGHT as u16);
        let head = NonNull::new(head as *mut _).unwrap();

        Ok(SkipList {
            height: AtomicUsize::new(1),
            head,
            arena,
            cmp,
        })
    }

    pub fn push(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut height = self.height.load(Ordering::Relaxed);
        let mut prev = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];
        let mut next = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];

        prev[height] = self.head.as_ptr();

        for h in (0..height).rev() {
            // [height-1,0]
            let (p, n) = self.find_splice_for_level(key, prev[h + 1], h); //[height,1]
            if p == n {
                return self.set_value(p, value);
            }
            prev[h] = p;
            next[h] = n;
        }
        let random_h = Self::random_height();
        let node = Node::new(&self.arena, key, value, random_h)?;

        while random_h > height {
            if let Err(h) = self.height.compare_exchange_weak(
                height,
                random_h,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                height = h;
            } else {
                break;
            };
        }

        for h in 0..random_h {
            loop {
                if prev[h].is_null() {
                    assert!(h > 1);
                    let (p, n) = self.find_splice_for_level(key, self.head.as_ptr(), h);
                    prev[h] = p;
                    next[h] = n;
                    assert_ne!(prev[h], next[h]);
                }

                let next_offset = self.arena.offset(next[h]).unwrap_or_default();
                node.tower[h].store(next_offset, Ordering::SeqCst);
                if unsafe { prev[h].as_ref() }.unwrap().tower[h].compare_exchange_weak(
                    next_offset,
                    self.arena.offset(node).unwrap(),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ).is_ok() {
                    break;
                }
                let (p, n) = self.find_splice_for_level(key, prev[h], h);
                if p == n {
                    assert!(h == 0);
                    return self.set_value(p, value);
                }
                prev[h] = p;
                next[h] = n;
            }
        }
        Ok(())
    }
    pub fn size(&self) -> usize {
        self.arena.len()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        if let Some(node) = self.find_or_next(key, false) {
            return node.get_value(&self.arena);
        }
        Ok(None)
    }
    pub fn get_or_next(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        if let Some(node) = self.find_or_next(key, true) {
            return node.get_value(&self.arena);
        }
        Ok(None)
    }
    pub fn get_next(&self, key: &[u8]) -> Result<&[u8]> {
        self.find_next(key)?.get_key(&self.arena)
    }
    pub fn get_prev(&self, key: &[u8]) -> Result<&[u8]> {
        if let Some(node) = self.find_prev(key) {
            return node.get_key(&self.arena);
        }
        Err(MorsSkipListError::KeyNotFound)
    }
    pub fn is_empty(&self) -> bool {
        self.find_last().is_none()
    }

    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }
}
impl SkipList {
    ///find the splice for the level
    fn find_splice_for_level<'a>(
        &self,
        key: &[u8],
        mut before_ptr: *const Node,
        height: usize,
    ) -> (*const Node, *const Node) {
        // (before,next)
        loop {
            if let Some(before) = unsafe { before_ptr.as_ref() } {
                if let Ok(next) = before.next(&self.arena, height) {
                    if let Ok(next_key_slice) = next.get_key(&self.arena) {
                        let next_ptr = next as *const _;
                        match (self.cmp)(key, next_key_slice) {
                            std::cmp::Ordering::Less => return (before_ptr, next_ptr),
                            std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                            std::cmp::Ordering::Greater => {
                                before_ptr = next_ptr;
                                continue;
                            }
                        }
                    }
                }
            };
            return (before_ptr, std::ptr::null());
        }
    }

    ///generate a random height
    ///a probability of `numerator/denominator`.
    ///for example,the probability of node.height==1 is 1/3
    ///the probability of node.height==2 is (1/3)^2, node.height==3 is (1/3)^3;
    #[inline]
    fn random_height() -> usize {
        const RANDOM_HEIGHT_NUMERATOR: u32 = 1;
        const RANDOM_HEIGHT_DENOMINATOR: u32 = 3;
        let mut rng = rand::thread_rng();
        let mut h = 1;
        while h < SKL_MAX_HEIGHT
            && rng.gen_ratio(RANDOM_HEIGHT_NUMERATOR, RANDOM_HEIGHT_DENOMINATOR)
        {
            h += 1;
        }
        h
    }
    fn set_value(&self, ptr: *const Node, value: &[u8]) -> Result<()> {
        if let Some(node) = unsafe { ptr.as_ref() } {
            if let Ok(Some(v)) = node.get_value(&self.arena) {
                if v == value {
                    return Ok(());
                }
            }
            Ok(node.set_value(&self.arena, value)?)
        } else {
            Err(MorsSkipListError::NullPointerError)
        }
    }

    fn find_or_next(&self, key: &[u8], allow_near: bool) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            match node.next(&self.arena, level) {
                Ok(next) => {
                    let next_key = next.get_key(&self.arena).unwrap();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Less => {
                            if level > 0 {
                                level -= 1;
                                continue;
                            } else {
                                if allow_near {
                                    return next.into();
                                }
                                return None;
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            return next.into();
                        }
                        std::cmp::Ordering::Greater => {
                            node = next;
                            continue;
                        }
                    }
                }
                Err(_) => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return None;
                    }
                }
            }
        }
    }
    fn find_next(&self, key: &[u8]) -> Result<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            match node.next(&self.arena, level) {
                Ok(next) => {
                    let next_key = next.get_key(&self.arena).unwrap_or_default();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Less => {
                            if level > 0 {
                                level -= 1;
                                continue;
                            } else {
                                return Ok(next);
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            return next.next(&self.arena, 0);
                        }
                        std::cmp::Ordering::Greater => {
                            node = next;
                            continue;
                        }
                    }
                }
                Err(_) => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return Err(MorsSkipListError::KeyNotFound);
                    }
                }
            }
        }
    }
    fn find_prev(&self, key: &[u8]) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let head_ptr = node as *const _;
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            match node.next(&self.arena, level) {
                Ok(next) => {
                    let next_key = next.get_key(&self.arena).unwrap();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Greater => {
                            //node.key <next.key < key
                            node = next;
                            continue;
                        }
                        _ => {}
                    }
                }
                Err(_) => {}
            }
            if level > 0 {
                level -= 1;
            } else {
                if head_ptr == node as *const _ {
                    return None;
                } else {
                    return node.into();
                }
            }
        }
    }
    fn find_last(&self) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            match node.next(&self.arena, level) {
                Ok(next) => {
                    node = next;
                }
                Err(_) => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return if node as *const _ == self.head.as_ptr() {
                            None
                        } else {
                            node.into()
                        };
                    }
                }
            }
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct NodeOffset(AtomicUsize);
impl Deref for NodeOffset {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl NodeOffset {
    fn get_node<'a>(&self, arena: &'a Arena) -> Result<&'a Node> {
        let offset = self.0.load(Ordering::Relaxed);
        if offset == 0 {
            return Err(MorsSkipListError::NullPointerError);
        }
        Ok(arena.get(offset)?)
    }
}
#[derive(Debug, Default)]
struct NextTower([NodeOffset; SKL_MAX_HEIGHT]);
impl Deref for NextTower {
    type Target = [NodeOffset; SKL_MAX_HEIGHT];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for NextTower {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Debug, Default)]
#[repr(C, align(8))]
pub(crate) struct Node {
    ///the value of the node
    value_slice: AtomicU64,
    ///the offset of the key in the arena
    key_offset: u32,
    ///the length of the key
    key_len: u16,
    ///the height of the node
    height: u16,
    ///the tower of the node
    tower: NextTower,
}
impl Node {
    pub(crate) fn new<'a>(
        arena: &'a Arena,
        key: &[u8],
        value: &[u8],
        height: usize,
    ) -> Result<&'a mut Self> {
        let node = arena.alloc_with(Self::default)?;
        let key_p = arena.alloc_slice_copy(key)?;
        node.key_offset = arena.offset_slice(key_p) as u32;

        node.key_len = key.len() as u16;
        node.height = height as u16;
        node.set_value(arena, value)?;
        Ok(node)
    }
    fn set_value(&self, arena: &Arena, value: &[u8]) -> Result<()> {
        let value_p = arena.alloc_slice_copy(value)?;
        let offset = arena.offset_slice(value_p);
        let v = (offset as u64) << 32 | value.len() as u64;
        self.value_slice.store(v, Ordering::Relaxed);
        Ok(())
    }

    fn get_key<'a>(&self, arena: &'a Arena) -> Result<&'a [u8]> {
        Ok(arena.get_slice::<u8>(self.key_offset as usize, self.key_len as usize)?)
    }
    fn get_value<'a>(&self, arena: &'a Arena) -> Result<Option<&'a [u8]>> {
        // let (offset, len) = self.value_slice();
        let v = self.value_slice.load(Ordering::Relaxed);
        let len = v as u32;
        if len == 0 {
            return Ok(None);
        }
        let offset = (v >> 32) as usize;
        Ok(Some(arena.get_slice::<u8>(offset as usize, len as usize)?))
    }
    #[inline]
    fn next<'a>(&self, arena: &'a Arena, level: usize) -> Result<&'a Node> {
        self.tower[level].get_node(arena)
    }

    fn set_height(&mut self, height: u16) {
        self.height = height;
    }
}
