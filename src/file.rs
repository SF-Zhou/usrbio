use super::{Error, Result};
use std::ffi::OsString;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct File {
    file: fs::File,
    mount_point: PathBuf,
}

impl File {
    /// # Safety
    ///
    /// Ensure the mount_point is correct.
    pub unsafe fn new(file: fs::File, mount_point: PathBuf) -> Result<File> {
        File::register_file(&file)?;
        Ok(File { file, mount_point })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        fs::OpenOptions::new().read(true).open_3fs_file(path)
    }

    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    fn register_file(file: &fs::File) -> Result<()> {
        let ret = unsafe { super::hf3fs_reg_fd(file.as_raw_fd(), 0) };
        if ret > 0 {
            Err(Error::RegisterFileFailed)
        } else {
            Ok(())
        }
    }

    pub fn extract_mount_point(path: &Path) -> Result<PathBuf> {
        let mut bytes = Vec::from(path.as_os_str().as_bytes());
        bytes.push(0);

        let max_len = bytes.len();
        let mut out = vec![0u8; max_len];
        let ret = unsafe {
            super::hf3fs_extract_mount_point(
                out.as_mut_ptr() as _,
                max_len as i32,
                bytes.as_ptr() as _,
            )
        };
        if ret <= 0 {
            return Err(Error::ExtractMountPointFailed);
        }
        out.truncate(ret as usize - 1);
        Ok(PathBuf::from(OsString::from_vec(out)))
    }
}

impl Drop for File {
    fn drop(&mut self) {
        unsafe { super::hf3fs_dereg_fd(self.as_raw_fd()) };
    }
}

impl Deref for File {
    type Target = fs::File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for File {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

pub trait Open3fsFile {
    fn open_3fs_file<P: AsRef<Path>>(&self, path: P) -> Result<File>
    where
        Self: Sized;
}

impl Open3fsFile for fs::OpenOptions {
    fn open_3fs_file<P: AsRef<Path>>(&self, path: P) -> Result<File>
    where
        Self: Sized,
    {
        let f = self.open(&path).map_err(Error::OpenFileFailed)?;
        let realpath = path
            .as_ref()
            .canonicalize()
            .map_err(|_| Error::ExtractMountPointFailed)?;
        let mount_point = File::extract_mount_point(&realpath)?;
        unsafe { File::new(f, mount_point) }
    }
}
