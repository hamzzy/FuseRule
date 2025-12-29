use std::sync::atomic::{AtomicU64, Ordering};

pub struct SystemMetrics {
    pub batches_processed: AtomicU64,
    pub activations_total: AtomicU64,
    pub agent_failures: AtomicU64,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            batches_processed: AtomicU64::new(0),
            activations_total: AtomicU64::new(0),
            agent_failures: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            activations_total: self.activations_total.load(Ordering::Relaxed),
            agent_failures: self.agent_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub batches_processed: u64,
    pub activations_total: u64,
    pub agent_failures: u64,
}

lazy_static::lazy_static! {
    pub static ref METRICS: SystemMetrics = SystemMetrics::new();
}
