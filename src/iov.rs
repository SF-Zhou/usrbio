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
