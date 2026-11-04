use super::*;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{mpsc, Arc},
    thread::JoinHandle,
};

/// A read request that keeps its registered file alive while queued on a worker.
#[derive(Debug)]
pub struct SendableReadJob {
    /// The registered 3FS file to read from.
    pub file: Arc<File>,
    /// Starting offset in bytes from the beginning of the file.
    pub offset: u64,
    /// Maximum number of bytes to read.
    pub length: usize,
}

impl ReadJob for SendableReadJob {
    fn file(&self) -> &File {
        &self.file
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn length(&self) -> usize {
        self.length
    }
}

/// A callback receiving a batch's jobs and their results in matching order.
///
/// For batches submitted to a worker, this runs on that worker's thread before
/// it processes the next batch. Result buffers are borrowed from the worker's
/// ring and are valid only during the call; copy any data that must be retained.
/// Check each [`ReadResult::ret`] for an individual I/O failure.
///
/// A panic in the callback terminates the worker and is propagated when the
/// worker is joined, including during drop.
pub type Callback = Box<dyn FnOnce(&[SendableReadJob], &[ReadResult]) + Sync + Send>;

/// A batch of reads and the callback used to deliver its results.
///
/// All files in a batch must belong to the same 3FS mount. A worker reports
/// `-EINVAL` for every job if mounts differ. The batch must also fit within
/// [`RingConfig::entries`] and [`RingConfig::buf_size`]; workers do not split it.
/// An empty batch invokes its callback with empty job and result slices.
pub struct BatchReadJobs {
    /// Read requests, in the order in which results will be delivered.
    pub jobs: Vec<SendableReadJob>,
    /// Callback invoked once when the batch is processed normally.
    pub callback: Callback,
}

impl BatchReadJobs {
    /// Consumes the batch and invokes its callback on the calling thread.
    ///
    /// Successful results are passed through unchanged. A batch-level error is
    /// converted with [`Error::errno`] and reported for every job through
    /// [`Self::set_error`].
    pub fn set_results(self, results: Result<Vec<ReadResult>>) {
        match results {
            Ok(r) => (self.callback)(&self.jobs, &r),
            Err(e) => self.set_error(e.errno()),
        }
    }

    /// Consumes the batch and reports the same error for every job.
    ///
    /// Pass a nonnegative error code. Each result has `ret = -errno` and an
    /// empty buffer. The callback runs on the calling thread.
    pub fn set_error(self, errno: i32) {
        let mut results = Vec::with_capacity(self.jobs.len());
        for _ in &self.jobs {
            results.push(ReadResult {
                ret: i64::from(-errno),
                buf: &[],
            });
        }
        (self.callback)(&self.jobs, &results);
    }
}

/// A background thread that processes queued read batches one at a time.
///
/// Each worker creates and caches a [`Ring`] on first use of a mount point.
/// Batches and their callbacks run sequentially on this thread. Dropping the
/// worker waits for queued batches and callbacks to finish, then joins it;
/// this can block and can panic if the worker panicked.
pub struct ReadWorker {
    config: RingConfig,
    sender: mpsc::Sender<BatchReadJobs>,
    handle: Option<JoinHandle<()>>,
}

impl ReadWorker {
    /// Starts a worker with a copy of `config`.
    ///
    /// Rings are created lazily, so this does not validate the configuration
    /// against a 3FS mount. Ring creation errors are delivered to batch callbacks.
    ///
    /// # Panics
    ///
    /// Panics if the operating system cannot create the worker thread.
    pub fn start(config: &RingConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<BatchReadJobs>();
        let config_clone = config.clone();
        let handle = std::thread::Builder::new()
            .name("ReadWorker".to_string())
            .spawn(move || Self::process(config_clone, receiver))
            .unwrap();

        Ok(Self {
            config: config.clone(),
            handle: Some(handle),
            sender,
        })
    }

    /// Queues a batch without waiting for its I/O or callback to finish.
    ///
    /// The queue is unbounded; callers should limit outstanding batches if
    /// they need to bound memory usage.
    ///
    /// # Panics
    ///
    /// Panics if the worker has stopped or its thread has terminated.
    pub fn enqueue(&self, job: BatchReadJobs) {
        self.sender.send(job).unwrap();
    }

    /// Returns the configuration used for this worker's rings.
    pub fn config(&self) -> &RingConfig {
        &self.config
    }

    /// Closes the queue and waits for the worker thread to exit.
    ///
    /// Pending batches and their callbacks finish before the thread exits
    /// normally. This does not cancel in-flight I/O. Calling this again after
    /// a successful join has no effect; subsequent calls to [`Self::enqueue`]
    /// panic. This method is also called when the worker is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the worker thread panicked, for example in a callback.
    pub fn stop_and_join(&mut self) {
        // Dropping the original sender lets the receiver drain pending batches
        // before returning an error. The replacement has no connected receiver.
        let _ = std::mem::replace(&mut self.sender, mpsc::channel::<BatchReadJobs>().0);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }

    fn process(config: RingConfig, receiver: mpsc::Receiver<BatchReadJobs>) {
        // Keep each ring on its owning worker thread and reuse it for that mount.
        let mut rings: HashMap<PathBuf, Ring> = HashMap::new();
        while let Ok(job) = receiver.recv() {
            let len = job.jobs.len();
            if len == 0 {
                job.set_error(0);
                continue;
            }

            let mount_point = job.jobs[0].file.mount_point();
            if !job
                .jobs
                .iter()
                .all(|job| job.file.mount_point() == mount_point)
            {
                job.set_error(22);
                continue;
            }

            if let Some(ring) = rings.get_mut(mount_point) {
                let results = ring.batch_read(&job.jobs);
                job.set_results(results);
            } else {
                let mut ring = match Ring::create(&config, mount_point) {
                    Ok(r) => r,
                    Err(e) => {
                        job.set_error(e.errno());
                        continue;
                    }
                };
                let mount_point = mount_point.to_owned();
                let results = ring.batch_read(&job.jobs);
                job.set_results(results);
                rings.insert(mount_point, ring);
            };
        }
    }
}

impl Drop for ReadWorker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}
