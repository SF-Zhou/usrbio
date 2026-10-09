use super::*;
use std::{ffi::CString, ops::Deref, os::unix::ffi::OsStrExt, path::Path};

#[repr(C)]
pub struct Iov {
    iov: hf3fs_iov,
}

impl Iov {
    pub fn create(
        mount_point: &Path,
        buf_size: usize,
        block_size: usize,
        numa: i32,
    ) -> Result<Self> {
        let c_str = CString::new(mount_point.as_os_str().as_bytes()).unwrap();

        let mut iov = hf3fs_iov::default();
        let ret =
            unsafe { hf3fs_iovcreate(&mut iov, c_str.as_ptr() as _, buf_size, block_size, numa) };

        if ret == 0 {
            Ok(Self { iov })
        } else {
            Err(Error::CretateIovFailed(-ret))
        }
    }

    /// Unlinks the backing `/dev/shm/hf3fs-iov-*` file, keeping the mapping and
    /// the hf3fs registration alive.
    ///
    /// Registration completes inside [`Iov::create`] and `hf3fs_prep_io` addresses
    /// the buffer by [`Iov::id`], so the name is dead weight afterwards. Dropping
    /// it lets the kernel reclaim the segment when the destructor cannot run
    /// (SIGKILL, OOM, a wedged worker); hf3fs does the same for iorings.
    ///
    /// Idempotent. Forecloses `hf3fs_iovopen`, which resolves by path.
    pub fn unlink(&mut self) {
        // SAFETY: `hf3fs_iovunlink` derefs `iov.iovh` unchecked; it is non-null
        // because `Self` only ever comes from `create()`, never `hf3fs_iovwrap`.
        unsafe { hf3fs_iovunlink(&mut self.iov) };
    }
}

impl Drop for Iov {
    fn drop(&mut self) {
        unsafe { hf3fs_iovdestroy(&mut self.iov) };
    }
}

impl Deref for Iov {
    type Target = hf3fs_iov;

    fn deref(&self) -> &Self::Target {
        &self.iov
    }
}

unsafe impl Send for Iov {}
unsafe impl Sync for Iov {}
