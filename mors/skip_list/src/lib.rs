use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
const SKL_MAX_HEIGHT: usize = 20; //<20 !=20
///a probability of `numerator/denominator`.
///for example,the probability of node.height==1 is 1/3
///the probability of node.height==2 is (1/3)^2, node.height==3 is (1/3)^3;
const RANDOM_HEIGHT_NUMERATOR: u32 = 1;
const RANDOM_HEIGHT_DENOMINATOR: u32 = 3;

extern crate thiserror;
use arena::Arena;
use error::MorsSkipListError;
use rand::Rng;

mod arena;
mod error;
mod impls;
type Result<T> = std::result::Result<T, MorsSkipListError>;

/// <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
/// <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
/// <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
/// <head> ----------> [2] --------------------------------------------------> [9] ---------->

struct Inner {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: Arena,
    cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering,
}
#[derive(Debug, Default)]
#[repr(C, align(8))]
pub(crate) struct Node {
    value_slice: AtomicU64,
    key_offset: u32,
    key_len: u16,
    height: u16,
    prev: NodeOffset,
    tower: Tower,
}
#[derive(Debug, Default)]
pub(crate) struct NodeOffset(AtomicUsize);
#[derive(Debug, Default)]
struct Tower([NodeOffset; SKL_MAX_HEIGHT]);

impl Inner {
    fn new(max_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Result<Self>
    where
        Self: Sized,
    {
        let arena = Arena::new(max_size)?;
        let head: &mut Node = arena.alloc_with(Node::default)?;
        head.set_height(SKL_MAX_HEIGHT as u16);
        let head = NonNull::new(head as *mut _).unwrap();

        Ok(Inner {
            height: AtomicUsize::new(1),
            head,
            arena,
            cmp,
        })
    }
    fn push(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut height = self.height();
        let mut prev = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];
        let mut next = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];
        prev[height] = self.head.as_ptr();
        for h in (0..height).rev() {
            let (p, n) = self.find_splice_for_level(key.into(), prev[h + 1], h);
            prev[h] = p;
            next[h] = n;
            if prev[h] == next[h] {
                self.try_set_value(prev[h], value);
                return Ok(());
            }
        }
        let random_height = Self::random_height();
        let node = Node::new(&self.arena, key, value, random_height)?;
        while random_height > height {
            match self.height.compare_exchange(
                height,
                random_height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    break;
                }
                Err(h) => {
                    height = h;
                }
            };
        }
        height = random_height;
        let node_offset = self.arena.offset(node).unwrap();
        Ok(for h in 0..height {
            loop {
                let prev_node = match unsafe { prev[h].as_ref() } {
                    Some(prev_node) => prev_node,
                    None => {
                        assert!(h > 1);
                        let (p, n) = self.find_splice_for_level(key.into(), self.head.as_ptr(), h);
                        prev[h] = p;
                        next[h] = n;
                        assert_ne!(prev[h], next[h]);
                        unsafe { &*prev[h] }
                    }
                };
                let mut next_offset = self.arena.offset(next[h]).unwrap_or_default();
                node.tower[h].store(next_offset, Ordering::SeqCst);
                if h == 0 {
                    loop {
                        let next_node = next[0];
                        let prev_offset = self.arena.offset(prev[0]).unwrap_or(self.head_offset());
                        node.prev.store(prev_offset, Ordering::SeqCst);
                        if !next_node.is_null() {
                            let next_node = unsafe { &*next_node };
                            match next_node.prev.compare_exchange(
                                prev_offset,
                                node_offset,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    break;
                                }
                                Err(_) => {
                                    let (p, n) = self.find_splice_for_level(key.into(), prev[0], 0);
                                    if p == n {
                                        self.try_set_value(prev[0], value);
                                        return Ok(());
                                    }
                                    prev[0] = p;
                                    next[0] = n;
                                    next_offset = self.arena.offset(next[0]).unwrap_or_default();
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }

                match prev_node.tower[h].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        break;
                    }
                    Err(_) => {
                        let (p, n) = self.find_splice_for_level(key.into(), prev_node as _, h);
                        prev[h] = p;
                        next[h] = n;
                        if prev[h] == next[h] {
                            assert!(h == 0);
                            self.try_set_value(prev[h], value);
                            return Ok(());
                        }
                    }
                };
            }
        })
    }
    fn find_splice_for_level<'a>(
        &self,
        key: &[u8],
        mut before_ptr: *const Node,
        height: usize,
    ) -> (*const Node, *const Node) {
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
    fn height(&self) -> usize {
        self.height.load(Ordering::Acquire)
    }
    #[inline]
    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut h = 1;
        while h < SKL_MAX_HEIGHT
            && rng.gen_ratio(RANDOM_HEIGHT_NUMERATOR, RANDOM_HEIGHT_DENOMINATOR)
        {
            h += 1;
        }
        return h;
    }
    fn try_set_value(&self, ptr: *const Node, value: &[u8]) {
        if let Some(node) = unsafe { ptr.as_ref() } {
            if let Ok(v) = node.get_value(&self.arena) {
                if v == value {
                    return;
                }
            };
            node.set_value(&self.arena, value);
        } else {
            unreachable!()
        }
    }
    fn find_or_near(&self, key: &[u8], allow_near: bool) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height() - 1;
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
        let mut level = self.height() - 1;
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
        let mut level = self.height() - 1;
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
        let mut level = self.height() - 1;
        loop {
            match node.next(&self.arena, level) {
                Ok(next) => {
                    node = next;
                }
                Err(_) => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return node.into();
                    }
                }
            }
        }
    }

    fn head_offset(&self) -> usize {
        self.arena.offset(self.head.as_ptr()).unwrap()
    }
}

impl Deref for NodeOffset {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl NodeOffset {
    fn new(arena: &Arena, node: &mut Node) -> Self {
        Self(AtomicUsize::new(
            arena.offset(node as *const _).unwrap_or_default(),
        ))
    }
    fn get_node<'a>(&self, arena: &'a Arena) -> Result<&'a Node> {
        let offset = self.0.load(Ordering::SeqCst);
        Ok(arena.get(offset as usize)?)
    }
}

impl Deref for Tower {
    type Target = [NodeOffset; SKL_MAX_HEIGHT];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Tower {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
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
        node.prev = NodeOffset(AtomicUsize::new(8));
        node.set_value(arena, value)?;
        Ok(node)
    }
    fn set_value(&self, arena: &Arena, value: &[u8]) -> Result<()> {
        let value_p = arena.alloc_slice_copy(value)?;
        let offset = arena.offset_slice(value_p);
        let v = (offset as u64) << 32 | value.len() as u64;
        self.value_slice.store(v, Ordering::SeqCst);
        Ok(())
    }

    fn value_slice(&self) -> (u32, u32) {
        let v = self.value_slice.load(Ordering::SeqCst);
        ((v >> 32) as u32, v as u32)
    }
    fn get_key<'a>(&self, arena: &'a Arena) -> Result<&'a [u8]> {
        Ok(arena.get_slice::<u8>(self.key_offset as usize, self.key_len as usize)?)
    }
    fn get_value<'a>(&self, arena: &'a Arena) -> Result<&'a [u8]> {
        let (offset, len) = self.value_slice();
        Ok(arena.get_slice::<u8>(offset as usize, len as usize)?)
    }
    #[inline]
    fn next<'a>(&self, arena: &'a Arena, level: usize) -> Result<&'a Node> {
        self.tower[level].get_node(arena)
    }
    fn prev<'a>(&self, arena: &'a Arena) -> Result<&'a Node> {
        self.prev.get_node(arena)
    }

    fn set_height(&mut self, height: u16) {
        self.height = height;
    }
}
