use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

// Simple histogram for latency tracking
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<(f64, AtomicU64)>, // (upper_bound, count)
}

impl Histogram {
    fn new() -> Self {
        // Standard Prometheus buckets: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
        let bounds = vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        Self {
            buckets: bounds.into_iter().map(|b| (b, AtomicU64::new(0))).collect(),
        }
    }

    fn record(&self, value: f64) {
        for (bound, count) in &self.buckets {
            if value <= *bound {
                count.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Value exceeds all buckets - increment the last one (infinity bucket)
        if let Some((_, count)) = self.buckets.last() {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> Vec<(f64, u64)> {
        self.buckets
            .iter()
            .map(|(bound, count)| (*bound, count.load(Ordering::Relaxed)))
            .collect()
    }

    fn to_prometheus(&self, name: &str, labels: &str) -> String {
        let snapshot = self.snapshot();
        let total: u64 = snapshot.iter().map(|(_, c)| c).sum();
        let mut output = format!("# HELP {}_seconds Duration histogram.\n", name);
        output.push_str(&format!("# TYPE {}_seconds histogram\n", name));
        for (bound, count) in snapshot {
            output.push_str(&format!(
                "{}{{le=\"{}\",{}}} {}\n",
                name, bound, labels, count
            ));
        }
        output.push_str(&format!(
            "{}{{le=\"+Inf\",{}}} {}\n",
            name, labels, total
        ));
        output
    }
}

pub struct SystemMetrics {
    pub batches_processed: AtomicU64,
    pub activations_total: AtomicU64,
    pub deactivations_total: AtomicU64,
    pub agent_failures: AtomicU64,
    pub evaluation_errors: AtomicU64,
    pub rule_evaluations: Mutex<HashMap<String, AtomicU64>>,
    pub rule_activations: Mutex<HashMap<String, AtomicU64>>,
    pub evaluation_duration: Histogram,
    pub rule_evaluation_duration: Mutex<HashMap<String, Histogram>>, // Per-rule histogram metrics
    pub agent_execution_duration: Mutex<HashMap<String, Histogram>>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            batches_processed: AtomicU64::new(0),
            activations_total: AtomicU64::new(0),
            deactivations_total: AtomicU64::new(0),
            agent_failures: AtomicU64::new(0),
            evaluation_errors: AtomicU64::new(0),
            rule_evaluations: Mutex::new(HashMap::new()),
            rule_activations: Mutex::new(HashMap::new()),
            evaluation_duration: Histogram::new(),
            rule_evaluation_duration: Mutex::new(HashMap::new()),
            agent_execution_duration: Mutex::new(HashMap::new()),
        }
    }
    
    pub fn record_evaluation_duration(&self, duration_secs: f64) {
        self.evaluation_duration.record(duration_secs);
    }
    
    pub fn record_agent_execution_duration(&self, agent_name: &str, duration_secs: f64) {
        let mut map = self.agent_execution_duration.lock().unwrap();
        let hist = map
            .entry(agent_name.to_string())
            .or_insert_with(|| Histogram::new());
        hist.record(duration_secs);
    }
    
    pub fn record_rule_evaluation(&self, rule_id: &str) {
        let mut map = self.rule_evaluations.lock().unwrap();
        map.entry(rule_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_rule_activation(&self, rule_id: &str) {
        let mut map = self.rule_activations.lock().unwrap();
        map.entry(rule_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_deactivation(&self) {
        self.deactivations_total.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_evaluation_error(&self) {
        self.evaluation_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_rule_evaluation_duration(&self, rule_id: &str, duration_secs: f64) {
        let mut map = self.rule_evaluation_duration.lock().unwrap();
        let hist = map
            .entry(rule_id.to_string())
            .or_insert_with(|| Histogram::new());
        hist.record(duration_secs);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let rule_evals: HashMap<String, u64> = self
            .rule_evaluations
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        let rule_acts: HashMap<String, u64> = self
            .rule_activations
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        
        MetricsSnapshot {
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            activations_total: self.activations_total.load(Ordering::Relaxed),
            deactivations_total: self.deactivations_total.load(Ordering::Relaxed),
            agent_failures: self.agent_failures.load(Ordering::Relaxed),
            evaluation_errors: self.evaluation_errors.load(Ordering::Relaxed),
            rule_evaluations: rule_evals,
            rule_activations: rule_acts,
        }
    }

    pub fn to_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = format!(
            "# HELP fuserule_batches_processed_total Total number of data batches ingested.\n\
             # TYPE fuserule_batches_processed_total counter\n\
             fuserule_batches_processed_total {}\n\
             # HELP fuserule_activations_total Total number of rule activations triggered.\n\
             # TYPE fuserule_activations_total counter\n\
             fuserule_activations_total {}\n\
             # HELP fuserule_deactivations_total Total number of rule deactivations.\n\
             # TYPE fuserule_deactivations_total counter\n\
             fuserule_deactivations_total {}\n\
             # HELP fuserule_agent_failures_total Total number of failed agent executions.\n\
             # TYPE fuserule_agent_failures_total counter\n\
             fuserule_agent_failures_total {}\n\
             # HELP fuserule_evaluation_errors_total Total number of rule evaluation errors.\n\
             # TYPE fuserule_evaluation_errors_total counter\n\
             fuserule_evaluation_errors_total {}\n",
            snapshot.batches_processed,
            snapshot.activations_total,
            snapshot.deactivations_total,
            snapshot.agent_failures,
            snapshot.evaluation_errors
        );
        
        // Add per-rule metrics
        output.push_str("# HELP fuserule_rule_evaluations_total Total evaluations per rule.\n");
        output.push_str("# TYPE fuserule_rule_evaluations_total counter\n");
        for (rule_id, count) in &snapshot.rule_evaluations {
            output.push_str(&format!(
                "fuserule_rule_evaluations_total{{rule_id=\"{}\"}} {}\n",
                rule_id, count
            ));
        }
        
        output.push_str("# HELP fuserule_rule_activations_total Total activations per rule.\n");
        output.push_str("# TYPE fuserule_rule_activations_total counter\n");
        for (rule_id, count) in &snapshot.rule_activations {
            output.push_str(&format!(
                "fuserule_rule_activations_total{{rule_id=\"{}\"}} {}\n",
                rule_id, count
            ));
        }
        
        // Add histogram metrics
        output.push_str(&self.evaluation_duration.to_prometheus(
            "fuserule_evaluation_duration",
            ""
        ));
        
        // Add per-rule evaluation duration histograms
        let rule_durations = self.rule_evaluation_duration.lock().unwrap();
        for (rule_id, hist) in rule_durations.iter() {
            output.push_str(&hist.to_prometheus(
                "fuserule_rule_evaluation_duration",
                &format!("rule_id=\"{}\"", rule_id)
            ));
        }
        
        let agent_durations = self.agent_execution_duration.lock().unwrap();
        for (agent_name, hist) in agent_durations.iter() {
            output.push_str(&hist.to_prometheus(
                "fuserule_agent_execution_duration",
                &format!("agent=\"{}\"", agent_name)
            ));
        }
        
        output
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub batches_processed: u64,
    pub activations_total: u64,
    pub deactivations_total: u64,
    pub agent_failures: u64,
    pub evaluation_errors: u64,
    pub rule_evaluations: HashMap<String, u64>,
    pub rule_activations: HashMap<String, u64>,
}

lazy_static::lazy_static! {
    pub static ref METRICS: SystemMetrics = SystemMetrics::new();
}
