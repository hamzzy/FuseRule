//! Rule coverage metrics tracking
//!
//! This module provides functionality to track which rules are executed
//! during testing and runtime, helping identify untested rules.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Tracks rule execution coverage
#[derive(Clone, Default)]
pub struct CoverageTracker {
    executions: Arc<Mutex<HashMap<String, u64>>>,
}

impl CoverageTracker {
    /// Create a new coverage tracker
    pub fn new() -> Self {
        Self {
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record that a rule was executed
    pub fn record_execution(&self, rule_id: &str) {
        let mut execs = self.executions.lock().unwrap();
        *execs.entry(rule_id.to_string()).or_insert(0) += 1;
    }

    /// Get execution count for a rule
    pub fn get_count(&self, rule_id: &str) -> u64 {
        let execs = self.executions.lock().unwrap();
        execs.get(rule_id).copied().unwrap_or(0)
    }

    /// Get coverage report
    pub fn get_report(&self, all_rule_ids: &[String]) -> CoverageReport {
        let execs = self.executions.lock().unwrap();
        let mut covered = 0;
        let mut uncovered = Vec::new();

        for rule_id in all_rule_ids {
            if execs.contains_key(rule_id) {
                covered += 1;
            } else {
                uncovered.push(rule_id.clone());
            }
        }

        CoverageReport {
            total: all_rule_ids.len(),
            covered,
            uncovered,
            coverage_percentage: if all_rule_ids.is_empty() {
                0.0
            } else {
                (covered as f64 / all_rule_ids.len() as f64) * 100.0
            },
            execution_counts: execs.clone(),
        }
    }
}

/// Coverage report
#[derive(Debug, Clone)]
pub struct CoverageReport {
    pub total: usize,
    pub covered: usize,
    pub uncovered: Vec<String>,
    pub coverage_percentage: f64,
    pub execution_counts: HashMap<String, u64>,
}

impl CoverageReport {
    /// Print a human-readable coverage report
    pub fn print(&self) {
        println!("📊 Rule Coverage Report");
        println!("{}", "=".repeat(50));
        println!("Total rules: {}", self.total);
        println!("Covered: {} ({:.1}%)", self.covered, self.coverage_percentage);
        println!("Uncovered: {}", self.uncovered.len());

        if !self.uncovered.is_empty() {
            println!("\nUncovered rules:");
            for rule_id in &self.uncovered {
                println!("  - {}", rule_id);
            }
        }

        println!("\nExecution counts:");
        for (rule_id, count) in &self.execution_counts {
            println!("  {}: {} execution(s)", rule_id, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coverage_tracker() {
        let tracker = CoverageTracker::new();
        tracker.record_execution("rule1");
        tracker.record_execution("rule1");
        tracker.record_execution("rule2");

        assert_eq!(tracker.get_count("rule1"), 2);
        assert_eq!(tracker.get_count("rule2"), 1);
        assert_eq!(tracker.get_count("rule3"), 0);

        let report = tracker.get_report(&["rule1".to_string(), "rule2".to_string(), "rule3".to_string()]);
        assert_eq!(report.total, 3);
        assert_eq!(report.covered, 2);
        assert_eq!(report.uncovered, vec!["rule3".to_string()]);
    }
}

