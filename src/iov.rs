use super::*;
use std::{ffi::CString, ops::Deref, os::unix::ffi::OsStrExt, path::Path};

/// An owned shared-memory data buffer registered with a 3FS FUSE process.
///
/// Dereferences to the native [`hf3fs_iov`] descriptor. Dropping the buffer
/// deregisters it and releases its local mapping. Raw I/O users must keep it
/// alive until all operations using it complete and synchronize buffer access.
/// Use [`Ring`] for managed reads and writes.
#[repr(C)]
pub struct Iov {
    iov: hf3fs_iov,
}

impl Iov {
    /// Allocates and registers a data buffer for `mount_point`.
    ///
    /// `buf_size` and `block_size` are in bytes. A zero `block_size` registers the
    /// whole buffer as one block; otherwise each I/O range must fit within one
    /// registration block. `numa` selects a NUMA node, or `-1` for the default
    /// allocation policy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CretateIovFailed`] if the native allocation or
    /// registration fails.
    ///
    /// # Panics
    ///
    /// Panics if `mount_point` contains an interior NUL byte.
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

    /// Removes the backing shared-memory name while retaining the mapping and
    /// 3FS registration.
    ///
    /// Registration completes inside [`Iov::create`], after which I/O identifies
    /// the buffer by its ID. Removing the `/dev/shm/hf3fs-iov-*` name allows the
    /// memory to be reclaimed once all mappings and open handles are released,
    /// including after a process exits without running destructors.
    ///
    /// Repeated calls have no additional effect. The buffer can no longer be
    /// reopened by name with [`hf3fs_iovopen`].
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
