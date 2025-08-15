#[derive(Debug)]
pub enum Error {
    OpenFileFailed(std::io::Error),
    ExtractMountPointFailed,
    RegisterFileFailed(i32),
    CretateIorFailed(i32),
    CretateIovFailed(i32),
    PrepareIOFailed(i32),
    SubmitIOsFailed(i32),
    WaitForIOsFailed(i32),
    ReadFailed(i32),
    WriteFailed(i32),
    InvalidArgument,
    InsufficientEntriesLength,
    InsufficientBufferLength,
    IOError(std::io::Error),
}

impl Error {
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

pub type Result<T> = std::result::Result<T, Error>;
