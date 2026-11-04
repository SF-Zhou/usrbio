/// Errors from file registration, ring management, and I/O operations.
///
/// Batch APIs also report individual I/O failures as negative completion values;
/// see [`crate::ReadResult`] and [`crate::Ring::batch_write`].
#[derive(Debug)]
pub enum Error {
    /// Opening the underlying file failed.
    OpenFileFailed(std::io::Error),
    /// Canonicalization or 3FS mount-point discovery failed.
    ExtractMountPointFailed,
    /// Registering a file descriptor failed with the native error code.
    RegisterFileFailed(i32),
    /// Creating a native request ring failed with the native error code.
    CretateIorFailed(i32),
    /// Creating a shared-memory data buffer failed with the native error code.
    CretateIovFailed(i32),
    /// Preparing an I/O request failed with the native error code.
    PrepareIOFailed(i32),
    /// Notifying the FUSE process of prepared requests failed.
    SubmitIOsFailed(i32),
    /// Waiting for I/O completions failed.
    WaitForIOsFailed(i32),
    /// A read failed; [`crate::Ring::read`] preserves the negative completion code.
    ReadFailed(i32),
    /// A write failed with the native error code.
    WriteFailed(i32),
    /// A read completion reported more bytes than requested.
    ReadLengthExceeded {
        /// Requested buffer length in bytes.
        expected: usize,
        /// Byte count reported by the completion.
        actual: i64,
    },
    /// A completion contained an out-of-range job index.
    InvalidUserData(usize),
    /// An argument is invalid, such as a zero worker count.
    InvalidArgument,
    /// A batch contains more jobs than the configured ring capacity.
    InsufficientEntriesLength,
    /// A batch requires more bytes than the configured data buffer can hold.
    InsufficientBufferLength,
    /// An error converted from [`std::io::Error`].
    IOError(std::io::Error),
}

impl Error {
    /// Maps this error to the integer code used by worker callbacks.
    ///
    /// Native-code variants return their stored value unchanged, including its
    /// sign. Invalid completions map to `EIO` (5); all other variants, including
    /// wrapped [`std::io::Error`] values, map to `EINVAL` (22).
    pub fn errno(&self) -> i32 {
        match self {
            Error::RegisterFileFailed(errno)
            | Error::CretateIorFailed(errno)
            | Error::CretateIovFailed(errno)
            | Error::PrepareIOFailed(errno)
            | Error::SubmitIOsFailed(errno)
            | Error::WaitForIOsFailed(errno)
            | Error::ReadFailed(errno)
            | Error::WriteFailed(errno) => *errno,
            Error::ReadLengthExceeded { .. } | Error::InvalidUserData(_) => 5,
            _ => 22,
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::IOError(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// A result whose error type is [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
