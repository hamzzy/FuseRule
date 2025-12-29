use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub struct WindowBuffer {
    window_duration: Duration,
    batches: VecDeque<(Instant, RecordBatch)>,
}

impl WindowBuffer {
    pub fn new(seconds: u64) -> Self {
        Self {
            window_duration: Duration::from_secs(seconds),
            batches: VecDeque::new(),
        }
    }

    pub fn add_batch(&mut self, batch: RecordBatch) {
        self.batches.push_back((Instant::now(), batch));
        self.prune();
    }

    pub fn prune(&mut self) {
        let now = Instant::now();
        while let Some((timestamp, _)) = self.batches.front() {
            if now.duration_since(*timestamp) > self.window_duration {
                self.batches.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn get_batches(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|(_, b)| b.clone()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}
