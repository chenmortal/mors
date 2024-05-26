#[macro_use]
extern crate lazy_static;
lazy_static! {
    pub static ref DEFAULT_PAGE_SIZE: usize =
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
}

