//! Cross-rule correlation for temporal patterns
//!
//! Enables CEP-style patterns:
//! - "Rule A AND Rule B fired within 5 minutes"
//! - "Rule A followed by Rule B within 10 seconds"
//! - Sequence patterns with time constraints

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Temporal window for correlation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalWindow {
    /// Window duration
    pub duration_seconds: i64,
    /// Sliding vs tumbling window
    pub window_type: WindowType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
    /// Sliding window (continuous)
    Sliding,
    /// Tumbling window (fixed intervals)
    Tumbling,
}

/// Correlation pattern definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CorrelationPattern {
    /// All rules must fire within time window
    All {
        rule_ids: Vec<String>,
        window: TemporalWindow,
    },

    /// Any N of M rules must fire within time window
    Any {
        rule_ids: Vec<String>,
        count: usize,
        window: TemporalWindow,
    },

    /// Rules must fire in sequence
    Sequence {
        rule_ids: Vec<String>,
        window: TemporalWindow,
    },

    /// Rule A but not Rule B
    NotPattern {
        required_rule: String,
        excluded_rule: String,
        window: TemporalWindow,
    },
}

/// Event representing a rule activation
#[derive(Debug, Clone)]
pub struct RuleActivationEvent {
    pub rule_id: String,
    pub timestamp: DateTime<Utc>,
    pub context: HashMap<String, serde_json::Value>,
}

/// Correlation engine for detecting temporal patterns
pub struct CorrelationEngine {
    /// Recent activations per rule (bounded)
    activation_history: HashMap<String, VecDeque<RuleActivationEvent>>,
    /// Maximum history to keep per rule
    max_history: usize,
    /// Registered correlation patterns
    patterns: Vec<CorrelationPattern>,
}

impl CorrelationEngine {
    pub fn new(max_history: usize) -> Self {
        Self {
            activation_history: HashMap::new(),
            max_history,
            patterns: Vec::new(),
        }
    }

    /// Register a correlation pattern
    pub fn add_pattern(&mut self, pattern: CorrelationPattern) {
        self.patterns.push(pattern);
    }

    /// Record a rule activation
    pub fn record_activation(&mut self, event: RuleActivationEvent) {
        let history = self
            .activation_history
            .entry(event.rule_id.clone())
            .or_insert_with(VecDeque::new);

        history.push_back(event);

        // Trim to max history
        while history.len() > self.max_history {
            history.pop_front();
        }
    }

    /// Check if any correlation patterns match
    pub fn check_patterns(&self) -> Vec<&CorrelationPattern> {
        self.patterns
            .iter()
            .filter(|pattern| self.matches_pattern(pattern))
            .collect()
    }

    fn matches_pattern(&self, pattern: &CorrelationPattern) -> bool {
        match pattern {
            CorrelationPattern::All { rule_ids, window } => {
                self.check_all_pattern(rule_ids, window)
            }

            CorrelationPattern::Any { rule_ids, count, window } => {
                self.check_any_pattern(rule_ids, *count, window)
            }

            CorrelationPattern::Sequence { rule_ids, window } => {
                self.check_sequence_pattern(rule_ids, window)
            }

            CorrelationPattern::NotPattern {
                required_rule,
                excluded_rule,
                window,
            } => self.check_not_pattern(required_rule, excluded_rule, window),
        }
    }

    fn check_all_pattern(&self, rule_ids: &[String], window: &TemporalWindow) -> bool {
        let now = Utc::now();
        let window_start = now - Duration::seconds(window.duration_seconds);

        // Check if all rules have at least one activation in the window
        rule_ids.iter().all(|rule_id| {
            self.activation_history
                .get(rule_id)
                .map(|history| {
                    history
                        .iter()
                        .any(|event| event.timestamp >= window_start)
                })
                .unwrap_or(false)
        })
    }

    fn check_any_pattern(&self, rule_ids: &[String], count: usize, window: &TemporalWindow) -> bool {
        let now = Utc::now();
        let window_start = now - Duration::seconds(window.duration_seconds);

        // Count how many rules have activations in the window
        let matching_count = rule_ids
            .iter()
            .filter(|rule_id| {
                self.activation_history
                    .get(*rule_id)
                    .map(|history| {
                        history
                            .iter()
                            .any(|event| event.timestamp >= window_start)
                    })
                    .unwrap_or(false)
            })
            .count();

        matching_count >= count
    }

    fn check_sequence_pattern(&self, rule_ids: &[String], window: &TemporalWindow) -> bool {
        if rule_ids.is_empty() {
            return false;
        }

        let now = Utc::now();
        let window_start = now - Duration::seconds(window.duration_seconds);

        // Find the most recent activation for each rule in the window
        let mut last_activations: Vec<Option<DateTime<Utc>>> = Vec::new();

        for rule_id in rule_ids {
            let last_activation = self
                .activation_history
                .get(rule_id)
                .and_then(|history| {
                    history
                        .iter()
                        .filter(|event| event.timestamp >= window_start)
                        .map(|event| event.timestamp)
                        .max()
                });

            last_activations.push(last_activation);
        }

        // Check if we have all activations
        if last_activations.iter().any(|opt| opt.is_none()) {
            return false;
        }

        // Check if they're in sequence (timestamps must be increasing)
        let mut prev_time: Option<DateTime<Utc>> = None;
        for time_opt in last_activations {
            if let Some(time) = time_opt {
                if let Some(prev) = prev_time {
                    if time <= prev {
                        return false;
                    }
                }
                prev_time = Some(time);
            }
        }

        true
    }

    fn check_not_pattern(
        &self,
        required_rule: &str,
        excluded_rule: &str,
        window: &TemporalWindow,
    ) -> bool {
        let now = Utc::now();
        let window_start = now - Duration::seconds(window.duration_seconds);

        // Required rule must have activation
        let has_required = self
            .activation_history
            .get(required_rule)
            .map(|history| {
                history
                    .iter()
                    .any(|event| event.timestamp >= window_start)
            })
            .unwrap_or(false);

        // Excluded rule must NOT have activation
        let has_excluded = self
            .activation_history
            .get(excluded_rule)
            .map(|history| {
                history
                    .iter()
                    .any(|event| event.timestamp >= window_start)
            })
            .unwrap_or(false);

        has_required && !has_excluded
    }

    /// Clean up old activations outside all windows
    pub fn cleanup_old_activations(&mut self, retention_seconds: i64) {
        let cutoff = Utc::now() - Duration::seconds(retention_seconds);

        for history in self.activation_history.values_mut() {
            while let Some(event) = history.front() {
                if event.timestamp < cutoff {
                    history.pop_front();
                } else {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_pattern() {
        let mut engine = CorrelationEngine::new(100);

        let pattern = CorrelationPattern::All {
            rule_ids: vec!["rule_a".to_string(), "rule_b".to_string()],
            window: TemporalWindow {
                duration_seconds: 60,
                window_type: WindowType::Sliding,
            },
        };

        engine.add_pattern(pattern);

        // Record activation for rule_a
        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_a".to_string(),
            timestamp: Utc::now(),
            context: HashMap::new(),
        });

        // Pattern should not match yet (only 1 of 2 rules)
        assert_eq!(engine.check_patterns().len(), 0);

        // Record activation for rule_b
        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_b".to_string(),
            timestamp: Utc::now(),
            context: HashMap::new(),
        });

        // Pattern should match now
        assert_eq!(engine.check_patterns().len(), 1);
    }

    #[test]
    fn test_sequence_pattern() {
        let mut engine = CorrelationEngine::new(100);

        let pattern = CorrelationPattern::Sequence {
            rule_ids: vec!["rule_a".to_string(), "rule_b".to_string()],
            window: TemporalWindow {
                duration_seconds: 60,
                window_type: WindowType::Sliding,
            },
        };

        engine.add_pattern(pattern);

        let now = Utc::now();

        // Record activations in order
        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_a".to_string(),
            timestamp: now,
            context: HashMap::new(),
        });

        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_b".to_string(),
            timestamp: now + Duration::seconds(5),
            context: HashMap::new(),
        });

        // Sequence pattern should match
        assert_eq!(engine.check_patterns().len(), 1);
    }

    #[test]
    fn test_not_pattern() {
        let mut engine = CorrelationEngine::new(100);

        let pattern = CorrelationPattern::NotPattern {
            required_rule: "rule_a".to_string(),
            excluded_rule: "rule_b".to_string(),
            window: TemporalWindow {
                duration_seconds: 60,
                window_type: WindowType::Sliding,
            },
        };

        engine.add_pattern(pattern);

        // Record only rule_a
        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_a".to_string(),
            timestamp: Utc::now(),
            context: HashMap::new(),
        });

        // Pattern should match (rule_a but not rule_b)
        assert_eq!(engine.check_patterns().len(), 1);

        // Record rule_b
        engine.record_activation(RuleActivationEvent {
            rule_id: "rule_b".to_string(),
            timestamp: Utc::now(),
            context: HashMap::new(),
        });

        // Pattern should not match anymore
        assert_eq!(engine.check_patterns().len(), 0);
    }
}
