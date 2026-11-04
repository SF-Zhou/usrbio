use super::*;

/// A positioned read request for [`Ring::batch_read`].
///
/// Implemented for `(&File, offset, length)` and `(F, offset, length)` where
/// `F: AsRef<File>`, including `Arc<File>`. Offsets and lengths are in bytes.
pub trait ReadJob {
    /// Returns the registered file to read, on the same mount as the ring.
    fn file(&self) -> &File;
    /// Returns the starting byte offset in the file.
    fn offset(&self) -> u64;
    /// Returns the maximum number of bytes to read.
    fn length(&self) -> usize;
}

impl ReadJob for (&File, u64, usize) {
    fn file(&self) -> &File {
        self.0
    }

    fn offset(&self) -> u64 {
        self.1
    }

    fn length(&self) -> usize {
        self.2
    }
}

impl<F: AsRef<File>> ReadJob for (F, u64, usize) {
    fn file(&self) -> &File {
        self.0.as_ref()
    }

    fn offset(&self) -> u64 {
        self.1
    }

    fn length(&self) -> usize {
        self.2
    }
}

/// The outcome of one read in a batch.
///
/// Results from [`Ring::batch_read`] borrow the ring's shared buffer. Copy data
/// that must be retained after the ring is reused. In a worker callback, the
/// borrowed data is available only for the duration of the callback.
pub struct ReadResult<'a> {
    /// Number of bytes read on success, or a negative error code from 3FS.
    ///
    /// A successful read may return fewer bytes than requested. `0` also
    /// represents end of file or a zero-length request.
    pub ret: i64,
    /// Bytes returned by the read, truncated to the successful byte count.
    /// Empty if the read fails or returns no data.
    pub buf: &'a [u8],
}

/// A positioned write request for [`Ring::batch_write`].
///
/// Implemented for `(&File, data, offset)` and `(F, data, offset)` where
/// `F: AsRef<File>`, including `Arc<File>`. `data` is a byte slice and `offset`
/// is measured in bytes.
pub trait WriteJob {
    /// Returns the registered file to write, on the same mount as the ring.
    fn file(&self) -> &File;
    /// Returns the data to copy into the ring's shared buffer and write.
    fn data(&self) -> &[u8];
    /// Returns the starting byte offset in the file.
    fn offset(&self) -> u64;
}

impl WriteJob for (&File, &[u8], u64) {
    fn file(&self) -> &File {
        self.0
    }

    fn data(&self) -> &[u8] {
        self.1
    }

    fn offset(&self) -> u64 {
        self.2
    }
}

impl<F: AsRef<File>> WriteJob for (F, &[u8], u64) {
    fn file(&self) -> &File {
        self.0.as_ref()
    }

    fn data(&self) -> &[u8] {
        self.1
    }

    fn offset(&self) -> u64 {
        self.2
    }
}
