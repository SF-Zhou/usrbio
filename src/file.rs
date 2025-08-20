use super::{Error, Result};
use std::ffi::OsString;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

/// A wrapper around the standard `fs::File` with additional 3fs-specific functionality.
/// This struct manages file operations and mount point tracking for the 3fs filesystem.
#[derive(Debug)]
pub struct File {
    file: fs::File,
    mount_point: PathBuf,
}

impl File {
    /// Creates a new `File` instance with the specified mount point.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the provided mount_point is valid and corresponds to the file.
    ///
    /// # Arguments
    /// * `file` - A standard filesystem file
    /// * `mount_point` - The mount point path for the file
    ///
    /// # Returns
    /// * `Result<File>` - A new File instance or an error
    pub unsafe fn new(file: fs::File, mount_point: PathBuf) -> Result<File> {
        File::register_file(&file)?;
        Ok(File { file, mount_point })
    }

    /// Opens a file in read-only mode.
    ///
    /// # Arguments
    /// * `path` - The path to the file to open
    ///
    /// # Returns
    /// * `Result<File>` - The opened file or an error
    pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        fs::OpenOptions::new().read(true).open_3fs_file(path)
    }

    /// Returns the mount point of the file.
    ///
    /// # Returns
    /// * `&Path` - Reference to the mount point path
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Registers the file with the 3fs system.
    ///
    /// # Arguments
    /// * `file` - The file to register
    ///
    /// # Returns
    /// * `Result<()>` - Success or an error if registration fails
    fn register_file(file: &fs::File) -> Result<()> {
        let ret = unsafe { super::hf3fs_reg_fd(file.as_raw_fd(), 0) };
        if ret > 0 {
            Err(Error::RegisterFileFailed(ret))
        } else {
            Ok(())
        }
    }

    /// Extracts the mount point from a given path.
    ///
    /// # Arguments
    /// * `path` - The path to extract the mount point from
    ///
    /// # Returns
    /// * `Result<PathBuf>` - The extracted mount point or an error
    pub fn extract_mount_point(path: impl AsRef<Path>) -> Result<PathBuf> {
        let mut bytes = Vec::from(path.as_ref().as_os_str().as_bytes());
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

    /// Extracts the mount point from a path with caching support.
    /// Maintains a cache of known mount points to avoid repeated extractions.
    ///
    /// # Arguments
    /// * `path` - The path to extract the mount point from
    ///
    /// # Returns
    /// * `Result<PathBuf>` - The extracted mount point or an error
    pub fn extract_mount_point_with_cache(path: impl AsRef<Path>) -> Result<PathBuf> {
        fn cache() -> &'static RwLock<Vec<PathBuf>> {
            static CACHE: OnceLock<RwLock<Vec<PathBuf>>> = OnceLock::new();
            CACHE.get_or_init(RwLock::default)
        }

        {
            let mountpoints = cache().read().unwrap();
            for mountpoint in mountpoints.iter() {
                if path.as_ref().starts_with(mountpoint) {
                    return Ok(PathBuf::from(mountpoint));
                }
            }
        }

        let mountpoint = Self::extract_mount_point(path)?;
        let mut cache = cache().write().unwrap();
        if !cache.contains(&mountpoint) {
            cache.push(mountpoint.clone());
        }

        Ok(mountpoint)
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

/// Trait for opening files in the 3fs filesystem.
/// This trait extends the standard file opening capabilities with 3fs-specific functionality.
pub trait Open3fsFile {
    /// Opens a file in the 3fs filesystem.
    ///
    /// # Arguments
    /// * `path` - The path to the file to open
    ///
    /// # Returns
    /// * `Result<File>` - The opened 3fs file or an error
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
        let mount_point = File::extract_mount_point_with_cache(&realpath)?;
        unsafe { File::new(f, mount_point) }
    }
}
