use super::*;
use std::{ffi::CString, ops::Deref, os::unix::ffi::OsStrExt, path::Path};

/// An owned native request/completion ring for one I/O direction.
///
/// Dereferences to [`hf3fs_ior`] for use with the raw FFI functions. Dropping the
/// ring destroys its native resources. Raw I/O users must finish outstanding
/// operations before dropping it and serialize calls that prepare I/O on the
/// same ring, even though this wrapper implements `Send` and `Sync`.
/// Use [`Ring`] for managed reads and writes.
#[repr(C)]
pub struct Ior {
    ior: hf3fs_ior,
}

impl Ior {
    /// Creates a request ring for a 3FS mount.
    ///
    /// `entries` is the requested queue capacity. `for_read` selects reads when
    /// true and writes when false. `io_depth` controls daemon-side batching:
    /// zero processes available requests immediately, positive values wait for
    /// a full batch of that size, and negative values cap each batch at their
    /// absolute value. With a negative depth, `timeout` is the maximum batching
    /// wait in milliseconds; zero adds no batching delay. It is not an I/O
    /// completion deadline.
    ///
    /// `numa` selects a NUMA node, or `-1` for the default allocation policy.
    /// `flags` is a bitmask passed to [`hf3fs_iorcreate4`], such as
    /// [`HF3FS_IOR_ALLOW_READ_UNCOMMITTED`] or [`HF3FS_IOR_FORBID_READ_HOLES`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::CretateIorFailed`] if native ring creation fails.
    ///
    /// # Panics
    ///
    /// Panics if `mount_point` contains an interior NUL byte.
    pub fn create(
        mount_point: &Path,
        entries: i32,
        for_read: bool,
        io_depth: i32,
        timeout: i32,
        numa: i32,
        flags: u64,
    ) -> Result<Self> {
        let c_str = CString::new(mount_point.as_os_str().as_bytes()).unwrap();

        let mut ior = hf3fs_ior::default();
        let ret = unsafe {
            hf3fs_iorcreate4(
                &mut ior,
                c_str.as_ptr() as _,
                entries as _,
                for_read,
                io_depth,
                timeout,
                numa,
                flags,
            )
        };
        if ret == 0 {
            Ok(Self { ior })
        } else {
            Err(Error::CretateIorFailed(-ret))
        }
    }
}

impl Drop for Ior {
    fn drop(&mut self) {
        unsafe { hf3fs_iordestroy(&mut self.ior) };
    }
}

impl Deref for Ior {
    type Target = hf3fs_ior;

    fn deref(&self) -> &Self::Target {
        &self.ior
    }
}

unsafe impl Send for Ior {}
unsafe impl Sync for Ior {}
