use crate::*;
use std::sync::atomic::AtomicUsize;

/// A pool that distributes whole read batches among workers in round-robin order.
///
/// Each worker owns its rings and runs its callbacks. Batches assigned to
/// different workers may complete in any order, and callbacks may run
/// concurrently. Dropping the pool joins its workers after their queues drain;
/// this can block and can panic if a worker panicked.
pub struct ReadWorkers {
    workers: Vec<ReadWorker>,
    index: AtomicUsize,
}

impl ReadWorkers {
    /// Starts `num_threads` workers, each using a copy of `config`.
    ///
    /// Rings are created lazily by each worker when a mount is first used.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] if `num_threads` is zero.
    ///
    /// # Panics
    ///
    /// Panics if the operating system cannot create a worker thread.
    pub fn start(config: &RingConfig, num_threads: usize) -> Result<Self> {
        if num_threads == 0 {
            return Err(Error::InvalidArgument);
        }
        let mut workers = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            workers.push(ReadWorker::start(config)?);
        }
        Ok(Self {
            workers,
            index: Default::default(),
        })
    }

    /// Queues a batch on the next worker without waiting for completion.
    ///
    /// The batch is not split across workers and must satisfy the requirements
    /// of [`BatchReadJobs`]. Worker queues are unbounded.
    ///
    /// # Panics
    ///
    /// Panics if the selected worker's thread has terminated.
    pub fn enqueue(&self, job: BatchReadJobs) {
        let index = self.index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.workers[index % self.workers.len()].enqueue(job);
    }

    /// Returns the ring configuration shared by all workers.
    pub fn config(&self) -> &RingConfig {
        self.workers[0].config()
    }
}

impl std::fmt::Debug for ReadWorkers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadWorkers")
            .field("config", &self.config())
            .field("num_threads", &self.workers.len())
            .field("index", &self.index)
            .finish()
    }
}
