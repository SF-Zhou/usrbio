use crate::{job::ReadResult, *};
use std::{ops::Deref, os::fd::AsRawFd, path::Path};

#[derive(Debug, Clone)]
pub struct RingConfig {
    pub entries: usize,
    pub buf_size: usize,
    pub io_depth: i32,
    pub timeout: i32,
    pub numa: i32,
    pub flags: u64,
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

pub struct Ring {
    config: RingConfig,
    read_ior: Ior,
    write_ior: Ior,
    iov: Iov,
    cqes: Vec<hf3fs_cqe>,
}

impl Ring {
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

    pub fn read(&mut self, file: &File, offset: u64, length: usize) -> Result<&[u8]> {
        let results = self.batch_read(&[(file, offset, length)])?;
        match &results[0] {
            e if e.ret < 0 => Err(Error::ReadFailed(e.ret as i32)),
            r => Ok(r.buf),
        }
    }

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

    pub fn write(&mut self, file: &File, buf: &[u8], offset: u64) -> Result<usize> {
        let results = self.batch_write(&[(file, buf, offset)])?;
        match results[0] {
            e if e < 0 => Err(Error::WriteFailed(-e as i32)),
            r => Ok(r as usize),
        }
    }

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
