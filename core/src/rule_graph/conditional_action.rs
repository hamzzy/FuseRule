//! Conditional actions - multiple actions per rule, gated by runtime context
//!
//! Enables rules to have different actions based on runtime conditions:
//!
//! ```yaml
//! actions:
//!   - if: "severity == 'critical'"
//!     agent: "pagerduty"
//!   - if: "severity == 'warning'"
//!     agent: "slack"
//!   - if: "severity == 'info'"
//!     agent: "logger"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A conditional action that executes based on runtime context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalAction {
    /// Condition that must be true to execute this action
    pub condition: ActionCondition,
    /// Agent to execute if condition is met
    pub agent: String,
    /// Optional parameters to pass to the agent
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    /// Priority (higher = executed first)
    #[serde(default)]
    pub priority: i32,
}

/// Condition for executing an action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ActionCondition {
    /// Always execute
    Always,

    /// Execute if field equals value
    FieldEquals {
        field: String,
        value: serde_json::Value,
    },

    /// Execute if field matches regex
    FieldMatches {
        field: String,
        pattern: String,
    },

    /// Execute if field is in range
    FieldInRange {
        field: String,
        min: f64,
        max: f64,
    },

    /// Execute if custom expression evaluates to true
    Expression {
        expr: String,
    },

    /// Logical AND of conditions
    And {
        conditions: Vec<ActionCondition>,
    },

    /// Logical OR of conditions
    Or {
        conditions: Vec<ActionCondition>,
    },

    /// Logical NOT
    Not {
        condition: Box<ActionCondition>,
    },
}

impl ActionCondition {
    /// Evaluate the condition against runtime context
    pub fn evaluate(&self, context: &HashMap<String, serde_json::Value>) -> bool {
        match self {
            ActionCondition::Always => true,

            ActionCondition::FieldEquals { field, value } => {
                context.get(field).map_or(false, |v| v == value)
            }

            ActionCondition::FieldMatches { field, pattern } => {
                if let Some(serde_json::Value::String(s)) = context.get(field) {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            ActionCondition::FieldInRange { field, min, max } => {
                if let Some(serde_json::Value::Number(n)) = context.get(field) {
                    if let Some(val) = n.as_f64() {
                        return val >= *min && val <= *max;
                    }
                }
                false
            }

            ActionCondition::Expression { expr } => {
                // Simple expression evaluation (could be enhanced with a proper evaluator)
                // For now, just try to extract field comparisons
                self.evaluate_simple_expression(expr, context)
            }

            ActionCondition::And { conditions } => {
                conditions.iter().all(|c| c.evaluate(context))
            }

            ActionCondition::Or { conditions } => {
                conditions.iter().any(|c| c.evaluate(context))
            }

            ActionCondition::Not { condition } => {
                !condition.evaluate(context)
            }
        }
    }

    fn evaluate_simple_expression(&self, expr: &str, context: &HashMap<String, serde_json::Value>) -> bool {
        // Simple expression parser for basic comparisons
        // Format: "field operator value" e.g., "severity == 'critical'"
        let parts: Vec<&str> = expr.split_whitespace().collect();

        if parts.len() != 3 {
            return false;
        }

        let field = parts[0];
        let operator = parts[1];
        let value_str = parts[2].trim_matches(|c| c == '\'' || c == '"');

        let context_value = match context.get(field) {
            Some(v) => v,
            None => return false,
        };

        match operator {
            "==" => {
                if let serde_json::Value::String(s) = context_value {
                    s == value_str
                } else if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64() == Some(v))
                } else {
                    false
                }
            }
            "!=" => {
                if let serde_json::Value::String(s) = context_value {
                    s != value_str
                } else if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64() != Some(v))
                } else {
                    false
                }
            }
            ">" => {
                if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64().map_or(false, |nv| nv > v))
                } else {
                    false
                }
            }
            "<" => {
                if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64().map_or(false, |nv| nv < v))
                } else {
                    false
                }
            }
            ">=" => {
                if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64().map_or(false, |nv| nv >= v))
                } else {
                    false
                }
            }
            "<=" => {
                if let serde_json::Value::Number(n) = context_value {
                    value_str.parse::<f64>().map_or(false, |v| n.as_f64().map_or(false, |nv| nv <= v))
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// Container for conditional actions
#[derive(Debug, Clone, Default)]
pub struct ConditionalActionSet {
    actions: Vec<ConditionalAction>,
}

impl ConditionalActionSet {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn add_action(&mut self, action: ConditionalAction) {
        self.actions.push(action);
        // Sort by priority (descending)
        self.actions.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Get all actions that should execute for the given context
    pub fn get_matching_actions(&self, context: &HashMap<String, serde_json::Value>) -> Vec<&ConditionalAction> {
        self.actions
            .iter()
            .filter(|action| action.condition.evaluate(context))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_condition() {
        let condition = ActionCondition::Always;
        let context = HashMap::new();
        assert!(condition.evaluate(&context));
    }

    #[test]
    fn test_field_equals() {
        let condition = ActionCondition::FieldEquals {
            field: "severity".to_string(),
            value: serde_json::json!("critical"),
        };

        let mut context = HashMap::new();
        context.insert("severity".to_string(), serde_json::json!("critical"));
        assert!(condition.evaluate(&context));

        context.insert("severity".to_string(), serde_json::json!("warning"));
        assert!(!condition.evaluate(&context));
    }

    #[test]
    fn test_field_in_range() {
        let condition = ActionCondition::FieldInRange {
            field: "price".to_string(),
            min: 100.0,
            max: 200.0,
        };

        let mut context = HashMap::new();
        context.insert("price".to_string(), serde_json::json!(150.0));
        assert!(condition.evaluate(&context));

        context.insert("price".to_string(), serde_json::json!(250.0));
        assert!(!condition.evaluate(&context));
    }

    #[test]
    fn test_expression_evaluation() {
        let condition = ActionCondition::Expression {
            expr: "severity == 'critical'".to_string(),
        };

        let mut context = HashMap::new();
        context.insert("severity".to_string(), serde_json::json!("critical"));
        assert!(condition.evaluate(&context));

        context.insert("severity".to_string(), serde_json::json!("warning"));
        assert!(!condition.evaluate(&context));
    }

    #[test]
    fn test_and_condition() {
        let condition = ActionCondition::And {
            conditions: vec![
                ActionCondition::FieldEquals {
                    field: "severity".to_string(),
                    value: serde_json::json!("critical"),
                },
                ActionCondition::FieldInRange {
                    field: "price".to_string(),
                    min: 100.0,
                    max: 200.0,
                },
            ],
        };

        let mut context = HashMap::new();
        context.insert("severity".to_string(), serde_json::json!("critical"));
        context.insert("price".to_string(), serde_json::json!(150.0));
        assert!(condition.evaluate(&context));

        context.insert("price".to_string(), serde_json::json!(250.0));
        assert!(!condition.evaluate(&context));
    }

    #[test]
    fn test_conditional_action_set() {
        let mut action_set = ConditionalActionSet::new();

        action_set.add_action(ConditionalAction {
            condition: ActionCondition::FieldEquals {
                field: "severity".to_string(),
                value: serde_json::json!("critical"),
            },
            agent: "pagerduty".to_string(),
            parameters: None,
            priority: 10,
        });

        action_set.add_action(ConditionalAction {
            condition: ActionCondition::FieldEquals {
                field: "severity".to_string(),
                value: serde_json::json!("warning"),
            },
            agent: "slack".to_string(),
            parameters: None,
            priority: 5,
        });

        let mut context = HashMap::new();
        context.insert("severity".to_string(), serde_json::json!("critical"));

        let matching = action_set.get_matching_actions(&context);
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].agent, "pagerduty");
    }
}
