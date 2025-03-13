use super::*;
use std::{collections::HashMap, path::PathBuf, sync::mpsc, thread::JoinHandle};

pub struct ReadWorker {
    config: RingConfig,
    sender: mpsc::Sender<BatchReadJob>,
    handle: Option<JoinHandle<()>>,
}

impl ReadWorker {
    pub fn start(config: &RingConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<BatchReadJob>();
        let config_clone = config.clone();
        let handle = std::thread::spawn(move || Self::process(config_clone, receiver));

        Ok(Self {
            config: config.clone(),
            handle: Some(handle),
            sender,
        })
    }

    pub fn enqueue(&self, job: BatchReadJob) {
        self.sender.send(job).unwrap();
    }

    pub fn config(&self) -> &RingConfig {
        &self.config
    }

    pub fn stop_and_join(&mut self) {
        // drop sender to stop.
        let _ = std::mem::replace(&mut self.sender, mpsc::channel::<BatchReadJob>().0);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }

    fn process(config: RingConfig, receiver: mpsc::Receiver<BatchReadJob>) {
        let mut cqes = vec![hf3fs_cqe::default(); config.entries];
        let mut rings: HashMap<PathBuf, Ring> = HashMap::new();
        while let Ok(job) = receiver.recv() {
            let len = job.jobs.len();
            let cqes = &mut cqes[..len];
            if len == 0 {
                job.set_error(cqes, 0);
                continue;
            }

            let mount_point = job.jobs[0].file.mount_point();
            if !job
                .jobs
                .iter()
                .all(|job| job.file.mount_point() == mount_point)
            {
                job.set_error(cqes, 22);
                continue;
            }

            if let Some(ring) = rings.get(mount_point) {
                ring.batch_read(job, cqes);
            } else {
                let ring = match Ring::create(&config, mount_point) {
                    Ok(r) => r,
                    Err(e) => {
                        job.set_error(cqes, e.errno());
                        continue;
                    }
                };
                let mount_point = mount_point.to_owned();
                ring.batch_read(job, cqes);
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
