use crate::{job::ReadResult, *};
use std::{ops::Deref, os::fd::AsRawFd, path::Path};

/// Configuration parameters for the I/O ring.
/// This structure defines the settings used to create and configure an I/O ring.
#[derive(Debug, Clone)]
pub struct RingConfig {
    /// Number of entries in the ring buffer
    pub entries: usize,
    /// Size of the I/O buffer in bytes
    pub buf_size: usize,
    /// Maximum I/O depth for concurrent operations
    pub io_depth: i32,
    /// Timeout value for I/O operations
    pub timeout: i32,
    /// NUMA node identifier (-1 for no NUMA awareness)
    pub numa: i32,
    /// Additional flags for ring configuration
    pub flags: u64,
    /// Block size for I/O operations
    pub block_size: usize,
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
        }
    }
}

/// An I/O ring that manages asynchronous read and write operations.
/// Provides facilities for both single and batch I/O operations with configurable parameters.
pub struct Ring {
    config: RingConfig,
    read_ior: Ior,
    write_ior: Ior,
    iov: Iov,
    cqes: Vec<hf3fs_cqe>,
}

impl Ring {
    /// Creates a new I/O ring with the specified configuration and mount point.
    ///
    /// # Arguments
    /// * `config` - Ring configuration parameters
    /// * `mount_point` - Mount point path for the filesystem
    ///
    /// # Returns
    /// * `Result<Self>` - A new Ring instance or an error
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
        let iov = Iov::create(mount_point, config.buf_size, config.block_size, config.numa)?;

        Ok(Self {
            config: config.clone(),
            read_ior,
            write_ior,
            iov,
            cqes: vec![hf3fs_cqe::default(); config.entries],
        })
    }

    /// Performs a single read operation.
    ///
    /// # Arguments
    /// * `file` - The file to read from
    /// * `offset` - Starting position for the read
    /// * `length` - Number of bytes to read
    ///
    /// # Returns
    /// * `Result<&[u8]>` - The read data as a byte slice or an error
    pub fn read(&mut self, file: &File, offset: u64, length: usize) -> Result<&[u8]> {
        let results = self.batch_read(&[(file, offset, length)])?;
        match &results[0] {
            e if e.ret < 0 => Err(Error::ReadFailed(e.ret as i32)),
            r => Ok(r.buf),
        }
    }

    /// Performs multiple read operations in a batch.
    ///
    /// # Arguments
    /// * `jobs` - Slice of read operations to perform
    ///
    /// # Returns
    /// * `Result<Vec<ReadResult<'_>>>` - Vector of read results or an error
    ///
    /// # Errors
    /// * Returns error if number of jobs exceeds ring entries
    /// * Returns error if total read length exceeds buffer size
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

        for cqe in &self.cqes {
            let result = &mut results[cqe.userdata as usize];
            result.ret = cqe.result;
            result.buf = &result.buf[..cqe.result as usize];
        }
        Ok(results)
    }

    /// Performs a single write operation.
    ///
    /// # Arguments
    /// * `file` - The file to write to
    /// * `buf` - Data to write
    /// * `offset` - Starting position for the write
    ///
    /// # Returns
    /// * `Result<usize>` - Number of bytes written or an error
    pub fn write(&mut self, file: &File, buf: &[u8], offset: u64) -> Result<usize> {
        let results = self.batch_write(&[(file, buf, offset)])?;
        match results[0] {
            e if e < 0 => Err(Error::WriteFailed(-e as i32)),
            r => Ok(r as usize),
        }
    }

    /// Performs multiple write operations in a batch.
    ///
    /// # Arguments
    /// * `jobs` - Slice of write operations to perform
    ///
    /// # Returns
    /// * `Result<Vec<i64>>` - Vector of write results (bytes written) or an error
    ///
    /// # Errors
    /// * Returns error if number of jobs exceeds ring entries
    /// * Returns error if total write length exceeds buffer size
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
            results[cqe.userdata as usize] = cqe.result;
        }
        Ok(results)
    }
}
