use super::{Error, Result};
use std::ffi::OsString;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

/// A file registered for USRBIO, together with its 3FS mount point.
///
/// Dereferences to [`std::fs::File`] for metadata and standard file operations.
/// Dropping this wrapper deregisters the descriptor before closing it. Keep the
/// wrapped descriptor unchanged for the wrapper's lifetime; replacing the inner
/// file through mutable dereferencing bypasses registration bookkeeping.
#[derive(Debug)]
pub struct File {
    file: fs::File,
    mount_point: PathBuf,
}

impl File {
    /// Registers an open file using an already known 3FS mount point.
    ///
    /// Prefer [`File::open`] or [`Open3fsFile::open_3fs_file`] when the mount point
    /// has not already been resolved.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RegisterFileFailed`] if descriptor registration fails.
    ///
    /// # Safety
    ///
    /// `mount_point` must identify the 3FS mount containing `file`. The descriptor
    /// must not be closed or deregistered externally while this wrapper is alive.
    pub unsafe fn new(file: fs::File, mount_point: PathBuf) -> Result<File> {
        File::register_file(&file)?;
        Ok(File { file, mount_point })
    }

    /// Opens a 3FS file for reading and registers it for USRBIO.
    ///
    /// Uses [`Open3fsFile::open_3fs_file`] to resolve the mount point. Use that
    /// method with [`std::fs::OpenOptions`] to open files for writing or creation.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the file, resolving its 3FS mount point, or
    /// registering its descriptor fails.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        fs::OpenOptions::new().read(true).open_3fs_file(path)
    }

    /// Returns the 3FS mount point used to register this file.
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    fn register_file(file: &fs::File) -> Result<()> {
        // Flag 1 disables descriptor duplication; this wrapper owns the original
        // descriptor and deregisters it before std::fs::File closes it.
        let ret = unsafe { super::hf3fs_reg_fd(file.as_raw_fd(), 1) };
        // Unlike the I/O functions, registration returns a positive errno.
        if ret > 0 {
            Err(Error::RegisterFileFailed(ret))
        } else {
            Ok(())
        }
    }

    /// Resolves the 3FS mount point containing `path`.
    ///
    /// Pass a canonical, absolute path so the output buffer, which is sized from
    /// the input path, can hold the mount point.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ExtractMountPointFailed`] if the native API cannot
    /// identify a 3FS mount for the path.
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

    /// Resolves a mount point, reusing a process-wide cache of known mounts.
    ///
    /// Pass a canonical, absolute path. Cache hits use path-prefix matching and
    /// do not query the filesystem again. The cache is never invalidated, so it
    /// assumes the mount layout remains stable during the process lifetime.
    ///
    /// # Errors
    ///
    /// Returns errors from [`File::extract_mount_point`] on a cache miss.
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
        // Deregister while the descriptor is still open; fields drop afterwards.
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

/// Extends [`std::fs::OpenOptions`] with 3FS mount discovery and registration.
pub trait Open3fsFile {
    /// Opens and registers a 3FS file using these open options.
    ///
    /// The [`std::fs::OpenOptions`] implementation canonicalizes the path after
    /// opening it and caches the discovered mount point.
    ///
    /// # Errors
    ///
    /// Returns [`Error::OpenFileFailed`] if opening fails,
    /// [`Error::ExtractMountPointFailed`] if canonicalization or mount discovery
    /// fails, or [`Error::RegisterFileFailed`] if registration fails. Creation
    /// and truncation options take effect before mount discovery and registration.
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
