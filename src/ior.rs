use super::*;
use std::{ffi::CString, ops::Deref, os::unix::ffi::OsStrExt, path::Path};

#[repr(C)]
pub struct Ior {
    ior: hf3fs_ior,
}

impl Ior {
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
