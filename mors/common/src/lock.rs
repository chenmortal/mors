use std::{
    collections::HashSet,
    fs::{File, OpenOptions, Permissions},
    io::{self, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use rustix::fs::flock;
use rustix::fs::FlockOperation::{
    NonBlockingLockExclusive, NonBlockingLockShared, Unlock,
};
pub struct DBLockGuard(Vec<DirLockGuard>);
pub struct DirLockGuard {
    dir_fd: File,
    pid_path: PathBuf,
    read_only: bool,
}
pub struct DBLockGuardBuilder {
    dirs: HashSet<PathBuf>,
    bypass_lock_guard: bool,
    read_only: bool,
}
impl DBLockGuardBuilder {
    pub fn new() -> Self {
        Self {
            dirs: HashSet::new(),
            bypass_lock_guard: false,
            read_only: false,
        }
    }
    pub fn add_dir(&mut self, dir: PathBuf) -> &mut Self {
        self.dirs.insert(dir);
        self
    }
    pub fn bypass_lock_guard(&mut self, bypass_lock_guard: bool) -> &mut Self {
        self.bypass_lock_guard = bypass_lock_guard;
        self
    }
    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }
    pub fn build(&self) -> io::Result<DBLockGuard> {
        let mut guards = Vec::new();
        for dir in &self.dirs {
            guards.push(DirLockGuard::acquire_lock(
                &dir,
                "LOCK",
                self.read_only,
            )?);
        }
        Ok(DBLockGuard(guards))
    }
}
impl DirLockGuard {
    fn acquire_lock(
        dir: &PathBuf,
        file_name: &str,
        read_only: bool,
    ) -> io::Result<Self> {
        let dir_fd = File::open(dir)?;
        flock(
            &dir_fd,
            if read_only {
                NonBlockingLockShared
            } else {
                NonBlockingLockExclusive
            },
        )
        .inspect_err(|e| {
            eprintln!("cannot acquire dir lock on {:?} because {e}", dir)
        })?;
        let pid_path = dir.join(file_name);
        if !read_only {
            let mut pid_f = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&pid_path)?;
            pid_f.set_permissions(Permissions::from_mode(0o666))?;
            pid_f.write_all(
                format!("{}", unsafe { libc::getpid() }).as_bytes(),
            )?;
        }
        Ok(Self {
            dir_fd,
            pid_path,
            read_only,
        })
    }
}
impl Drop for DirLockGuard {
    fn drop(&mut self) {
        if !self.read_only {
            if let Err(e) = std::fs::remove_file(&self.pid_path) {
                eprintln!(
                    "cannot remove pid file {:?} because {e}",
                    self.pid_path
                );
            };
        }
        if let Err(e) = flock(&self.dir_fd, Unlock) {
            eprintln!("cannot release dir lock because {e}");
        };
    }
}
