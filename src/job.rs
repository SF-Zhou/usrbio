use super::*;
use std::sync::Arc;

#[derive(Debug)]
pub struct ReadJob {
    pub file: Arc<File>,
    pub offset: u64,
    pub length: usize,
}

pub type Callback = Box<dyn FnOnce(&[ReadJob], &[u8], &[hf3fs_cqe]) + Sync + Send>;

pub struct BatchReadJob {
    pub jobs: Vec<ReadJob>,
    pub callback: Callback,
}

impl BatchReadJob {
    pub fn set_error(self, cqes: &mut [hf3fs_cqe], errno: i32) {
        for cqe in cqes.iter_mut() {
            cqe.result = -errno as i64;
        }
        (self.callback)(&self.jobs, &[], cqes);
    }
}
