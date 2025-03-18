use crate::*;
use std::{ffi::c_void, ops::Deref, os::fd::AsRawFd, path::Path, ptr::null_mut};

#[derive(Debug, Clone)]
pub struct RingConfig {
    pub entries: usize,
    pub buf_size: usize,
    pub for_read: bool,
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
            for_read: true,
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
    ior: Ior,
    iov: Iov,
}

impl Ring {
    pub fn create(config: &RingConfig, mount_point: &Path) -> Result<Self> {
        let ior = Ior::create(
            mount_point,
            config.entries as _,
            config.for_read,
            config.io_depth,
            config.timeout,
            config.numa,
            config.flags,
        )?;
        let iov = Iov::create(mount_point, config.buf_size, config.block_size, config.numa)?;

        Ok(Self {
            config: config.clone(),
            ior,
            iov,
        })
    }

    pub fn batch_read(&self, job: BatchReadJob, cqes: &mut [hf3fs_cqe]) {
        assert_eq!(job.jobs.len(), cqes.len());
        assert!(cqes.len() <= self.config.entries);

        let total_len = job.jobs.iter().map(|job| job.length).sum::<usize>();
        assert!(total_len < self.config.buf_size);

        let mut ptr = self.iov.base as *mut c_void;
        let mut prepare_errno = None;
        for (idx, read_job) in job.jobs.iter().enumerate() {
            let ret = unsafe {
                hf3fs_prep_io(
                    self.ior.deref(),
                    self.iov.deref(),
                    true,
                    ptr,
                    read_job.file.as_raw_fd(),
                    read_job.offset as usize,
                    read_job.length as u64,
                    idx as _,
                )
            };
            if ret < 0 {
                prepare_errno = Some(-ret);
                break;
            }
            ptr = unsafe { ptr.add(read_job.length) };
        }
        if let Some(errno) = prepare_errno.take() {
            return self.set_error(job, cqes, errno);
        }

        let ret = unsafe { hf3fs_submit_ios(self.ior.deref()) };
        if ret != 0 {
            return self.set_error(job, cqes, -ret);
        }

        let ret = unsafe {
            hf3fs_wait_for_ios(
                self.ior.deref(),
                cqes.as_mut_ptr(),
                cqes.len() as _,
                cqes.len() as _,
                std::ptr::null_mut(),
            )
        };
        if ret < 0 {
            return self.set_error(job, cqes, -ret);
        }
        let buf = unsafe { std::slice::from_raw_parts(self.iov.base, self.config.buf_size) };
        cqes.sort_by_key(|cqe| cqe.userdata as usize);
        (job.callback)(&job.jobs, buf, cqes);
    }

    pub fn write(&self, file: &File, buf: &[u8], offset: u64) -> Result<usize> {
        assert!(buf.len() <= self.config.buf_size);
        let slice = unsafe { std::slice::from_raw_parts_mut(self.iov.base, buf.len()) };
        slice.copy_from_slice(buf);

        let ret = unsafe {
            hf3fs_prep_io(
                self.ior.deref(),
                self.iov.deref(),
                false,
                self.iov.base as _,
                file.as_raw_fd(),
                offset as usize,
                buf.len() as u64,
                null_mut(),
            )
        };
        if ret < 0 {
            return Err(Error::PrepareIOFailed(-ret));
        }

        let ret = unsafe { hf3fs_submit_ios(self.ior.deref()) };
        if ret != 0 {
            return Err(Error::SubmitIOsFailed(-ret));
        }

        let mut cqes = [hf3fs_cqe::default()];
        let ret = unsafe {
            hf3fs_wait_for_ios(
                self.ior.deref(),
                cqes.as_mut_ptr(),
                cqes.len() as _,
                cqes.len() as _,
                std::ptr::null_mut(),
            )
        };
        if ret < 0 {
            return Err(Error::WaitForIOsFailed(-ret));
        }

        if cqes[0].result < 0 {
            return Err(Error::WriteFailed(-cqes[0].result as _));
        }

        Ok(cqes[0].result as _)
    }

    #[inline(always)]
    fn set_error(&self, job: BatchReadJob, cqes: &mut [hf3fs_cqe], errno: i32) {
        for (_, cqe) in job.jobs.iter().zip(cqes.iter_mut()) {
            cqe.result = errno as _;
        }
        let buf = unsafe { std::slice::from_raw_parts(self.iov.base, self.config.buf_size) };
        (job.callback)(&job.jobs, buf, cqes);
    }
}
