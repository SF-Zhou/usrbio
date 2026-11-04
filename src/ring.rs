use crate::{job::ReadResult, *};
use std::{ops::Deref, os::fd::AsRawFd, path::Path};

/// Shared-memory and batching settings used by [`Ring`] and [`ReadWorker`].
///
/// The defaults allow up to 1,024 jobs and 4 MiB of data per batch, with no
/// fixed batch size or NUMA placement.
#[derive(Debug, Clone)]
pub struct RingConfig {
    /// Maximum number of jobs per batch. Defaults to 1,024.
    ///
    /// Must be positive and fit in an `i32`, as required by the C API.
    pub entries: usize,
    /// Size of the shared data buffer in bytes. Defaults to 4 MiB.
    ///
    /// Must be nonzero. The sum of requested lengths in a batch must fit here.
    pub buf_size: usize,
    /// Controls how the 3FS worker groups prepared requests. Defaults to `0`.
    ///
    /// `0` processes available requests without a fixed batch size. A positive
    /// value waits for groups of exactly that many requests; each batch must
    /// contain a multiple of that many nonempty jobs to avoid waiting forever.
    /// A negative value processes at most `-io_depth` requests per group,
    /// optionally waiting for more requests according to [`Self::timeout`].
    pub io_depth: i32,
    /// Maximum batching delay in milliseconds when [`Self::io_depth`] is negative.
    ///
    /// Must be nonnegative. `0` (the default) disables this delay. This setting
    /// is not a deadline for completing I/O; [`Ring`] waits without a completion
    /// timeout.
    pub timeout: i32,
    /// NUMA node for shared-memory allocation, or `-1` for no explicit placement.
    /// Defaults to `-1`.
    pub numa: i32,
    /// Flags passed to the 3FS read and write rings. Defaults to `0`.
    ///
    /// See [`HF3FS_IOR_ALLOW_READ_UNCOMMITTED`] and [`HF3FS_IOR_FORBID_READ_HOLES`].
    pub flags: u64,
    /// Shared-memory registration block size in bytes.
    ///
    /// `0` (the default) registers the whole buffer as one block. A nonzero
    /// value must not exceed [`Self::buf_size`]. Each job's data must fit within
    /// one registration block; [`Ring`] packs jobs consecutively without adding
    /// padding or splitting requests at block boundaries.
    pub block_size: usize,
    /// Unlink the data buffer's `/dev/shm/hf3fs-iov-*` file after registration,
    /// while keeping the mapping and registration alive. Defaults to `true`;
    /// see [`Iov::unlink`] for lifecycle details.
    pub unlink_iov: bool,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            entries: 1024,
            buf_size: 4 << 20,
            io_depth: 0,
            timeout: 0,
            numa: -1,
            flags: 0,
            block_size: 0,
            unlink_iov: true,
        }
    }
}

/// Blocking single and batch I/O over 3FS shared-memory rings.
///
/// A `Ring` owns separate read and write rings and one shared data buffer for a
/// single 3FS mount. Successful calls return after their requests complete. Use
/// [`ReadWorker`] or [`ReadWorkers`] to perform reads on background threads.
///
/// Read results borrow the shared buffer, so they must be consumed or copied
/// before the ring can be used again. All files in an operation must belong to
/// the mount passed to [`Ring::create`].
pub struct Ring {
    config: RingConfig,
    read_ior: Ior,
    write_ior: Ior,
    iov: Iov,
    cqes: Vec<hf3fs_cqe>,
}

impl Ring {
    /// Creates read and write rings and a shared data buffer for `mount_point`.
    ///
    /// Obtain the mount point from [`File::mount_point`]. The configuration is
    /// cloned, so subsequent changes to `config` do not affect this ring.
    ///
    /// # Errors
    ///
    /// Returns an error if 3FS cannot create either ring or allocate and register
    /// the shared data buffer.
    ///
    /// # Panics
    ///
    /// Panics if `mount_point` contains an interior NUL byte.
    pub fn create(config: &RingConfig, mount_point: &Path) -> Result<Self> {
        let read_ior = Ior::create(
            mount_point,
            config.entries as _,
            true,
            config.io_depth,
            config.timeout,
            config.numa,
            config.flags,
        )?;
        let write_ior = Ior::create(
            mount_point,
            config.entries as _,
            false,
            config.io_depth,
            config.timeout,
            config.numa,
            config.flags,
        )?;
        let mut iov = Iov::create(mount_point, config.buf_size, config.block_size, config.numa)?;
        if config.unlink_iov {
            iov.unlink();
        }

        Ok(Self {
            config: config.clone(),
            read_ior,
            write_ior,
            iov,
            cqes: vec![hf3fs_cqe::default(); config.entries],
        })
    }

    /// Reads up to `length` bytes at the byte `offset`, waiting for completion.
    ///
    /// The returned slice borrows this ring's shared buffer and may be shorter
    /// than requested, for example at end of file. A zero-length request returns
    /// an empty slice. The file cursor is not changed.
    ///
    /// # Errors
    ///
    /// Returns errors from [`Self::batch_read`], or [`Error::ReadFailed`] if the
    /// individual read fails.
    pub fn read(&mut self, file: &File, offset: u64, length: usize) -> Result<&[u8]> {
        let results = self.batch_read(&[(file, offset, length)])?;
        match &results[0] {
            e if e.ret < 0 => Err(Error::ReadFailed(e.ret as i32)),
            r => Ok(r.buf),
        }
    }

    /// Reads a batch and waits for all nonempty requests to complete.
    ///
    /// Jobs may be tuples of `(&File, offset, length)`; see [`ReadJob`]. All files
    /// must belong to this ring's mount. Results are returned in input order,
    /// even if requests complete out of order. Zero-length jobs succeed with an
    /// empty buffer and are not submitted to 3FS.
    ///
    /// Each result borrows this ring's shared buffer. Check [`ReadResult::ret`]
    /// for individual failures: an `Ok` batch can still contain negative error
    /// codes, and successful reads may be shorter than requested.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InsufficientEntriesLength`] if the job count exceeds
    /// [`RingConfig::entries`], or [`Error::InsufficientBufferLength`] if the sum
    /// of requested lengths exceeds [`RingConfig::buf_size`]. Also returns
    /// errors for failed preparation, submission, or completion waiting, and
    /// for invalid completion metadata. Errors do not roll back reads already
    /// prepared or submitted, or cancel and drain outstanding requests.
    pub fn batch_read(&mut self, jobs: &[impl ReadJob]) -> Result<Vec<ReadResult<'_>>> {
        let count = jobs.len();
        if count > self.config.entries {
            return Err(Error::InsufficientEntriesLength);
        }

        let total_len = jobs.iter().map(|job| job.length()).sum::<usize>();
        if total_len > self.config.buf_size {
            return Err(Error::InsufficientBufferLength);
        }

        let mut buf = unsafe { std::slice::from_raw_parts(self.iov.base, self.config.buf_size) };
        let mut submitted = 0;
        let mut results = Vec::with_capacity(count);
        for (idx, job) in jobs.iter().enumerate() {
            if job.length() == 0 {
                results.push(ReadResult { ret: 0, buf: &[] });
                continue;
            }

            let ret = unsafe {
                hf3fs_prep_io(
                    self.read_ior.deref(),
                    self.iov.deref(),
                    true,
                    buf.as_ptr() as _,
                    job.file().as_raw_fd(),
                    job.offset() as usize,
                    job.length() as u64,
                    idx as _,
                )
            };
            if ret < 0 {
                return Err(Error::PrepareIOFailed(-ret));
            }
            results.push(ReadResult {
                ret: 0,
                buf: &buf[..job.length()],
            });
            buf = &buf[job.length()..];
            submitted += 1;
        }

        if submitted == 0 {
            return Ok(results);
        }

        let ret = unsafe { hf3fs_submit_ios(self.read_ior.deref()) };
        if ret != 0 {
            return Err(Error::SubmitIOsFailed(-ret));
        }

        let ret = unsafe {
            self.cqes.set_len(submitted);
            hf3fs_wait_for_ios(
                self.read_ior.deref(),
                self.cqes.as_mut_ptr(),
                submitted as _,
                submitted as _,
                std::ptr::null_mut(),
            )
        };
        if ret < 0 {
            return Err(Error::WaitForIOsFailed(-ret));
        }

        // The original job index travels in userdata; completion order may differ.
        for cqe in &self.cqes {
            let idx = cqe.userdata as usize;
            let result = results.get_mut(idx).ok_or(Error::InvalidUserData(idx))?;
            if cqe.result > result.buf.len() as i64 {
                return Err(Error::ReadLengthExceeded {
                    expected: result.buf.len(),
                    actual: cqe.result,
                });
            }
            let len = cqe.result.max(0) as usize;
            result.ret = cqe.result;
            result.buf = &result.buf[..len];
        }
        Ok(results)
    }

    /// Writes `buf` at the byte `offset`, waiting for completion.
    ///
    /// Copies the data into this ring's shared buffer before submitting it.
    /// Returns the number of bytes written, which may be less than `buf.len()`.
    /// An empty buffer returns `0`. The file cursor is not changed.
    ///
    /// # Errors
    ///
    /// Returns errors from [`Self::batch_write`], or [`Error::WriteFailed`] if the
    /// individual write fails.
    pub fn write(&mut self, file: &File, buf: &[u8], offset: u64) -> Result<usize> {
        let results = self.batch_write(&[(file, buf, offset)])?;
        match results[0] {
            e if e < 0 => Err(Error::WriteFailed(-e as i32)),
            r => Ok(r as usize),
        }
    }

    /// Writes a batch and waits for all nonempty requests to complete.
    ///
    /// Jobs may be tuples of `(&File, data, offset)`; see [`WriteJob`]. All files
    /// must belong to this ring's mount. Data is copied into the shared buffer
    /// before submission. Empty writes succeed with `0` and are not submitted.
    ///
    /// Results are returned in input order. Each value is a nonnegative byte
    /// count or a negative error code from 3FS; an `Ok` batch does not imply that
    /// every write succeeded or wrote all requested bytes. The batch is not
    /// atomic, and overlapping writes have no guaranteed execution order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InsufficientEntriesLength`] if the job count exceeds
    /// [`RingConfig::entries`], or [`Error::InsufficientBufferLength`] if the sum
    /// of data lengths exceeds [`RingConfig::buf_size`]. Also returns errors
    /// for failed preparation, submission, or completion waiting, and for
    /// invalid completion metadata. Errors do not roll back writes already
    /// prepared or submitted, or cancel and drain outstanding requests.
    pub fn batch_write(&mut self, jobs: &[impl WriteJob]) -> Result<Vec<i64>> {
        let count = jobs.len();
        if count > self.config.entries {
            return Err(Error::InsufficientEntriesLength);
        }

        let total_len = jobs.iter().map(|job| job.data().len()).sum::<usize>();
        if total_len > self.config.buf_size {
            return Err(Error::InsufficientBufferLength);
        }

        let mut buf =
            unsafe { std::slice::from_raw_parts_mut(self.iov.base, self.config.buf_size) };
        let mut submitted = 0;
        let mut results = vec![0; count];
        for (idx, job) in jobs.iter().enumerate() {
            if job.data().is_empty() {
                continue;
            }

            let len = job.data().len();
            buf[..len].copy_from_slice(job.data());

            let ret = unsafe {
                hf3fs_prep_io(
                    self.write_ior.deref(),
                    self.iov.deref(),
                    false,
                    buf.as_ptr() as _,
                    job.file().as_raw_fd(),
                    job.offset() as usize,
                    len as u64,
                    idx as _,
                )
            };
            if ret < 0 {
                return Err(Error::PrepareIOFailed(-ret));
            }

            buf = &mut buf[len..];
            submitted += 1;
        }

        if submitted == 0 {
            return Ok(results);
        }

        let ret = unsafe { hf3fs_submit_ios(self.write_ior.deref()) };
        if ret != 0 {
            return Err(Error::SubmitIOsFailed(-ret));
        }

        let ret = unsafe {
            self.cqes.set_len(submitted);
            hf3fs_wait_for_ios(
                self.write_ior.deref(),
                self.cqes.as_mut_ptr(),
                submitted as _,
                submitted as _,
                std::ptr::null_mut(),
            )
        };
        if ret < 0 {
            return Err(Error::WaitForIOsFailed(-ret));
        }

        for cqe in &self.cqes {
            let idx = cqe.userdata as usize;
            *results.get_mut(idx).ok_or(Error::InvalidUserData(idx))? = cqe.result;
        }
        Ok(results)
    }
}
