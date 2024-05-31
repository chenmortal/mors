use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::mem::size_of;

use mors_common::DEFAULT_PAGE_SIZE;

use std::ptr::{self, NonNull};
use std::sync::atomic::AtomicUsize;

use crate::error::ArenaError;

const CHUNK_ALIGN: usize = 16;

const DEFAULT_ALIGN: usize = 8;

type Result<T> = std::result::Result<T, ArenaError>;
#[derive(Debug)]
pub struct Arena {
    start: NonNull<u8>,
    ptr_offset: AtomicUsize,
    end: NonNull<u8>,
    layout: Layout,
}
impl Arena {
    pub fn new(size: usize) -> Result<Arena> {
        let chunk_align = CHUNK_ALIGN;
        let size = size as usize;
        let mut request_size = Self::round_up_to(size, chunk_align);
        debug_assert_eq!(chunk_align % CHUNK_ALIGN, 0);
        if request_size >= DEFAULT_PAGE_SIZE.to_owned() {
            request_size = Self::round_up_to(request_size, DEFAULT_PAGE_SIZE.to_owned());
        }
        debug_assert_eq!(request_size % CHUNK_ALIGN, 0);

        let layout = Layout::from_size_align(request_size, chunk_align)?;
        let (data, end) = unsafe {
            let data_ptr = alloc(layout);
            if data_ptr.is_null() {
                handle_alloc_error(layout);
            }
            let data = NonNull::new_unchecked(data_ptr);

            let end_ptr = data.as_ptr().add(layout.size());
            let end = NonNull::new_unchecked(end_ptr);
            (data, end)
        };
        debug_assert_eq!((data.as_ptr() as usize) % layout.align(), 0);
        debug_assert_eq!((end.as_ptr() as usize) % CHUNK_ALIGN, 0);
        let ptr_offset = AtomicUsize::new(0);
        let s = Self {
            start: data,
            ptr_offset,
            end,
            layout,
        };
        Ok(s)
    }
    pub fn alloc<T>(&self, value: T) -> Result<&mut T> {
        self.alloc_with(|| value)
    }
    pub fn alloc_with<F, T>(&self, f: F) -> Result<&mut T>
    where
        F: FnOnce() -> T,
    {
        #[inline(always)]
        unsafe fn inner_write<T, F>(dst: *mut T, f: F)
        where
            F: FnOnce() -> T,
        {
            ptr::write(dst, f())
        }
        let layout = Layout::new::<T>();
        let p = self.alloc_layout(layout)?;
        let dst = p.as_ptr() as *mut T;
        unsafe {
            inner_write(dst, f);
            Ok(&mut *dst)
        }
    }

    pub fn get_mut<T>(&self, offset: usize) -> Result<&mut T> {
        unsafe {
            let ptr = self.start.as_ptr().add(offset as usize);

            if ptr.add(size_of::<T>()) > self.end.as_ptr() {
                Err(ArenaError::OffsetOutOfBound {
                    offset,
                    size: std::mem::size_of::<T>(),
                    limit: self.layout.size(),
                })
            } else {
                Ok(&mut *(ptr as *mut T))
            }
        }
    }
    pub fn get<T>(&self, offset: usize) -> Result<&T> {
        unsafe {
            let ptr = self.start.as_ptr().add(offset as usize);
            if ptr.add(size_of::<T>()) > self.end.as_ptr() {
                Err(ArenaError::OffsetOutOfBound {
                    offset,
                    size: std::mem::size_of::<T>(),
                    limit: self.layout.size(),
                })
            } else {
                Ok(&*(ptr as *mut T))
            }
        }
    }
    pub fn get_slice<T>(&self, offset: usize, len: usize) -> Result<&[T]> {
        if len == 0 {
            return Err(ArenaError::ZeroLengthError);
        }
        unsafe {
            let ptr = self.start.as_ptr().add(offset);

            if ptr.add(len * size_of::<T>()) > self.end.as_ptr() {
                Err(ArenaError::OffsetOutOfBound {
                    offset,
                    size: len * size_of::<T>(),
                    limit: self.layout.size(),
                })
            } else {
                Ok(std::slice::from_raw_parts(ptr as *const T, len))
            }
        }
    }
    pub fn offset<N>(&self, ptr: *const N) -> Result<usize> {
        if ptr.is_null() {
            return Err(ArenaError::NullPointerError);
        }

        let ptr_addr = ptr as usize;
        let start_addr = self.start.as_ptr() as usize;
        debug_assert!(ptr_addr >= start_addr);
        Ok(ptr_addr - start_addr)
    }
    fn alloc_layout(&self, layout: Layout) -> Result<NonNull<u8>> {
        debug_assert!(DEFAULT_ALIGN.is_power_of_two());
        let layout = layout.align_to(DEFAULT_ALIGN).unwrap();
        let end_ptr = self.end.as_ptr();
        let start_ptr = self.start.as_ptr();
        let alloc_size = Self::round_up_to(layout.size(), layout.align());
        let old_ptr = unsafe {
            self.start.as_ptr().add(
                self.ptr_offset
                    .fetch_add(alloc_size, std::sync::atomic::Ordering::AcqRel)
                    as usize,
            )
        };
        debug_assert_eq!(old_ptr as usize % 8, 0);
        unsafe {
            let new_ptr = old_ptr.add(alloc_size);
            if new_ptr > end_ptr {
                return Err(ArenaError::SizeTooSmall {
                    to_write: layout.size(),
                    new_total: new_ptr.offset_from(start_ptr) as usize,
                    limit: self.layout.size(),
                });
            }
            Ok(NonNull::new_unchecked(old_ptr))
        }
    }
    #[inline(always)]
    pub fn alloc_slice_copy<T: Copy>(&self, src: &[T]) -> Result<NonNull<T>> {
        let layout = Layout::for_value(src);
        let dst = self.alloc_layout(layout)?.cast::<T>();
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len());
        }
        Ok(dst)
    }
    pub(crate) fn offset_slice<T>(&self, ptr: NonNull<T>) -> usize {
        let ptr_addr = ptr.as_ptr() as usize;
        let start_addr = self.start.as_ptr() as usize;
        debug_assert!(ptr_addr >= start_addr);
        ptr_addr - start_addr
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.ptr_offset.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn max_size(&self) -> usize {
        self.layout.size()
    }

    #[inline(always)]
    pub fn alloc_slice_clone<T: Clone>(&self, src: &[T]) -> Result<NonNull<T>> {
        let layout = Layout::for_value(src);
        let dst = self.alloc_layout(layout)?.cast::<T>();
        unsafe {
            for (i, val) in src.iter().cloned().enumerate() {
                ptr::write(dst.as_ptr().add(i), val);
            }
        }
        Ok(dst)
    }
    ///Round up to the nearest multiple of divisor
    #[inline(always)]
    fn round_up_to(n: usize, divisor: usize) -> usize {
        debug_assert!(divisor > 0);
        debug_assert!(divisor.is_power_of_two());
        (n + divisor - 1) & !(divisor - 1)
    }
}
impl Drop for Arena {
    fn drop(&mut self) {
        unsafe {
            // 因为在这里指向的元素是u8 实现了 Trait Copy , 所以 drop_in_place 在这里不会有任何操作,所以直接用dealloc
            // Because the element pointed to here is u8 that implements the Trait Copy, drop_in_place does nothing here,so use dealloc
            // ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
            //     self.start.as_ptr(),
            //     self.layout.size(),
            // ));

            dealloc(self.start.as_ptr(), self.layout);
        }
    }
}

#[test]
fn test_round_up_to() {
    assert_eq!(Arena::round_up_to(1, 8), 8);
    assert_eq!(Arena::round_up_to(8, 8), 8);
    assert_eq!(Arena::round_up_to(9, 8), 16);
    assert_eq!(Arena::round_up_to(16, 8), 16);
    assert_eq!(Arena::round_up_to(17, 8), 24);
    assert_eq!(Arena::round_up_to(24, 8), 24);
    assert_eq!(Arena::round_up_to(25, 8), 32);
    assert_eq!(Arena::round_up_to(32, 8), 32);
    assert_eq!(Arena::round_up_to(33, 8), 40);
    assert_eq!(Arena::round_up_to(40, 8), 40);
    assert_eq!(Arena::round_up_to(41, 8), 48);
    assert_eq!(Arena::round_up_to(48, 8), 48);
    assert_eq!(Arena::round_up_to(49, 8), 56);
    assert_eq!(Arena::round_up_to(56, 8), 56);
    assert_eq!(Arena::round_up_to(57, 8), 64);
    assert_eq!(Arena::round_up_to(64, 8), 64);
    assert_eq!(Arena::round_up_to(65, 8), 72);
    assert_eq!(Arena::round_up_to(72, 8), 72);
    assert_eq!(Arena::round_up_to(73, 8), 80);
    assert_eq!(Arena::round_up_to(80, 8), 80);
    assert_eq!(Arena::round_up_to(81, 8), 88);
    assert_eq!(Arena::round_up_to(88, 8), 88);
    assert_eq!(Arena::round_up_to(89, 8), 96);
    assert_eq!(Arena::round_up_to(96, 8), 96);
    assert_eq!(Arena::round_up_to(97, 8), 104);
    assert_eq!(Arena::round_up_to(104, 8), 104);
    assert_eq!(Arena::round_up_to(105, 8), 112);
    assert_eq!(Arena::round_up_to(112, 8), 112);
}

#[test]
fn test_alloc() {
    let arena = Arena::new(100).unwrap();
    let value = 42;
    let ptr = arena.alloc(value).unwrap();
    assert_eq!(*ptr, value);
}

#[test]
fn test_alloc_with() {
    let arena = Arena::new(100).unwrap();
    let value = 42;
    let ptr = arena.alloc_with(|| value);
    assert_eq!(*ptr.unwrap(), value);
}
#[test]
fn test_over_alloc() {
    let arena = Arena::new(100).unwrap();
    let value = 42;
    let ptr = arena.alloc(value).unwrap();
    let offset = arena.offset(ptr).unwrap();
    let mut_ref = arena.get_mut::<i32>(offset).unwrap();
    *mut_ref = 24;
    assert_eq!(*ptr, 24);
}
#[test]
fn test_over_size() {
    for size in 0..1024 {
        let arena = Arena::new(size).unwrap();
        for i in 0..usize::MAX {
            if arena.len() + size_of::<usize>() > arena.max_size() {
                break;
            }
            assert!(arena.alloc(i).is_ok());
        }
        assert!(arena.alloc(11 as usize).is_err());
    }
}
#[test]
fn test_get_mut() {
    let arena = Arena::new(4).unwrap();
    arena.alloc(1 as usize).unwrap();
    let value = 42 as usize;
    let ptr = arena.alloc(value).unwrap();
    let offset = arena.offset(ptr).unwrap();

    let mut_ref = arena.get_mut::<usize>(offset).unwrap();
    assert_eq!(*mut_ref, value);
    *mut_ref = 24;

    assert_eq!(*ptr, 24);
}

#[test]
fn test_get_slice() {
    let arena = Arena::new(16).unwrap();
    let values: [usize; 2] = [1, 2];
    let ptr = arena.alloc_slice_copy(&values).unwrap();
    let offset = arena.offset_slice(ptr);
    let slice = arena.get_slice::<usize>(offset, values.len()).unwrap();
    assert_eq!(slice, &values);
    assert!(arena.get_slice::<usize>(offset + 1, values.len()).is_err());
}

#[test]
fn test_offset() {
    let arena = Arena::new(100).unwrap();
    let value = 42;
    let ptr = arena.alloc(value).unwrap();
    let offset = arena.offset(ptr).unwrap();
    let ptr_from_offset = arena.get::<i32>(offset).unwrap();
    assert_eq!(ptr, ptr_from_offset);
}

#[test]
fn test_slice_string() {
    let s = String::from("1,2,3,4");
    let slice = s.as_bytes();
    let arena = Arena::new(100).unwrap();
    let p = arena.alloc_slice_copy(slice).unwrap();
    let offset = arena.offset_slice(p);
    let k = arena.get_slice::<u8>(offset, slice.len()).unwrap();
    assert_eq!(String::from_utf8_lossy(k), s);
}
