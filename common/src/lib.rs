// #[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicUsize, Ordering};

pub mod mmap;
pub mod lock;
mod sys;
pub mod compress;
pub mod closer;
pub mod util;
pub mod bloom;
pub mod rayon;
pub mod kv;
pub mod test;
pub mod ts;
pub mod file_id;
pub mod histogram;
// lazy_static! {
//     pub static ref DEFAULT_PAGE_SIZE: usize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
// }
pub fn page_size() -> usize {
    static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);
  
    match PAGE_SIZE.load(Ordering::Relaxed) {
        0 => {
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };

            PAGE_SIZE.store(page_size, Ordering::Relaxed);

            page_size
        }
        page_size => page_size,
    }
}
