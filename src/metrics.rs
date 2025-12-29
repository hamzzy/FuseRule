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

    pub fn to_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        format!(
            "# HELP fuserule_batches_processed_total Total number of data batches ingested.\n\
             # TYPE fuserule_batches_processed_total counter\n\
             fuserule_batches_processed_total {}\n\
             # HELP fuserule_activations_total Total number of rule activations triggered.\n\
             # TYPE fuserule_activations_total counter\n\
             fuserule_activations_total {}\n\
             # HELP fuserule_agent_failures_total Total number of failed agent executions.\n\
             # TYPE fuserule_agent_failures_total counter\n\
             fuserule_agent_failures_total {}\n",
            snapshot.batches_processed,
            snapshot.activations_total,
            snapshot.agent_failures
        )
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
