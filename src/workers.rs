use crate::*;
use std::sync::atomic::AtomicUsize;

pub struct ReadWorkers {
    workers: Vec<ReadWorker>,
    index: AtomicUsize,
}

impl ReadWorkers {
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

    pub fn enqueue(&self, job: BatchReadJob) {
        let index = self.index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.workers[index % self.workers.len()].enqueue(job);
    }

    pub fn config(&self) -> &RingConfig {
        self.workers[0].config()
    }
}
