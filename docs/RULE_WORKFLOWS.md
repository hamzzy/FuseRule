# Rule Workflows - Decision Graphs & Advanced Semantics

FuseRule transforms from simple alerts into expressive decision workflows through advanced rule semantics:

- **Rule Chaining**: Rules trigger other rules in a DAG
- **Conditional Actions**: Multiple actions per rule, gated by runtime context
- **Dynamic Predicates**: Load predicates from DB/S3, A/B testing, feature flags
- **Cross-Rule Correlation**: Temporal patterns like "A AND B within 5 minutes"

**Outcome**: Rules become decision workflows, not just alerts.

## Table of Contents

- [Rule Chaining](#rule-chaining)
- [Conditional Actions](#conditional-actions)
- [Dynamic Predicates](#dynamic-predicates)
- [Cross-Rule Correlation](#cross-rule-correlation)
- [Complete Examples](#complete-examples)

## Rule Chaining

### Overview

Rules can trigger other rules in a directed acyclic graph (DAG), enabling complex multi-step workflows.

### Example: Trading Alert Pipeline

```text
high_price → check_volume → alert_trader
          ↘
            check_volatility → alert_risk_management
```

### Configuration

```yaml
rule_chains:
  - rule_id: "high_price"
    downstream_rules:
      - "check_volume"
      - "check_volatility"
    trigger_conditions:
      check_volume:
        type: "Always"
      check_volatility:
        type: "FieldEquals"
        field: "symbol"
        value: "AAPL"

  - rule_id: "check_volume"
    downstream_rules:
      - "alert_trader"
    trigger_conditions:
      alert_trader:
        type: "Expression"
        expr: "volume > 10000"

  - rule_id: "check_volatility"
    downstream_rules:
      - "alert_risk_management"
```

### Programmatic Usage

```rust
use fuse_rule_core::rule_graph::chain::{RuleChain, ChainedRule, TriggerCondition};
use std::collections::HashMap;

let mut chain = RuleChain::new();

// Add high_price → check_volume chain
let mut trigger_conditions = HashMap::new();
trigger_conditions.insert(
    "check_volume".to_string(),
    TriggerCondition::Always,
);

chain.add_chain(ChainedRule {
    rule_id: "high_price".to_string(),
    downstream_rules: vec!["check_volume".to_string()],
    trigger_conditions,
}).unwrap();

// Validate no cycles
chain.validate_dag().unwrap();

// Get topological order
let execution_order = chain.topological_sort().unwrap();
println!("Execution order: {:?}", execution_order);
```

### DAG Validation

The system automatically validates that rule chains form a DAG (no cycles):

```rust
// This will fail with "Cycle detected"
let mut chain = RuleChain::new();

chain.add_chain(ChainedRule {
    rule_id: "rule_a".to_string(),
    downstream_rules: vec!["rule_b".to_string()],
    trigger_conditions: HashMap::new(),
}).unwrap();

chain.add_chain(ChainedRule {
    rule_id: "rule_b".to_string(),
    downstream_rules: vec!["rule_a".to_string()],
    trigger_conditions: HashMap::new(),
}).unwrap();

// ❌ This will return an error
chain.validate_dag().unwrap_err();
```

### Trigger Conditions

Four types of trigger conditions control when downstream rules fire:

#### 1. Always Trigger

```rust
TriggerCondition::Always
```

#### 2. Field Equals

```rust
TriggerCondition::FieldEquals {
    field: "severity".to_string(),
    value: "critical".to_string(),
}
```

#### 3. Custom Expression

```rust
TriggerCondition::Expression {
    expr: "volume > 10000 AND price > 100".to_string(),
}
```

#### 4. Consecutive Count

```rust
TriggerCondition::ConsecutiveCount {
    count: 3,  // Trigger after 3 consecutive activations
}
```

## Conditional Actions

### Overview

Rules can have multiple actions, each gated by runtime context. This replaces "one rule = one action" with flexible decision logic.

### Example: Severity-Based Routing

```yaml
rules:
  - id: "anomaly_detector"
    name: "Anomaly Detector"
    predicate: "z_score > 3.0"
    conditional_actions:
      - condition:
          type: "FieldEquals"
          field: "severity"
          value: "critical"
        agent: "pagerduty"
        priority: 10

      - condition:
          type: "FieldEquals"
          field: "severity"
          value: "warning"
        agent: "slack"
        priority: 5

      - condition:
          type: "FieldEquals"
          field: "severity"
          value: "info"
        agent: "logger"
        priority: 1
```

### Programmatic Usage

```rust
use fuse_rule_core::rule_graph::conditional_action::{
    ConditionalAction, ActionCondition, ConditionalActionSet
};
use std::collections::HashMap;

let mut action_set = ConditionalActionSet::new();

// Critical severity → PagerDuty
action_set.add_action(ConditionalAction {
    condition: ActionCondition::FieldEquals {
        field: "severity".to_string(),
        value: serde_json::json!("critical"),
    },
    agent: "pagerduty".to_string(),
    parameters: None,
    priority: 10,
});

// Warning severity → Slack
action_set.add_action(ConditionalAction {
    condition: ActionCondition::FieldEquals {
        field: "severity".to_string(),
        value: serde_json::json!("warning"),
    },
    agent: "slack".to_string(),
    parameters: None,
    priority: 5,
});

// Evaluate at runtime
let mut context = HashMap::new();
context.insert("severity".to_string(), serde_json::json!("critical"));

let matching_actions = action_set.get_matching_actions(&context);
for action in matching_actions {
    println!("Execute agent: {}", action.agent);
}
```

### Condition Types

#### Field Equals

```rust
ActionCondition::FieldEquals {
    field: "region".to_string(),
    value: serde_json::json!("us-east-1"),
}
```

#### Field in Range

```rust
ActionCondition::FieldInRange {
    field: "price".to_string(),
    min: 100.0,
    max: 200.0,
}
```

#### Field Matches Regex

```rust
ActionCondition::FieldMatches {
    field: "error_message".to_string(),
    pattern: r"timeout|connection.*failed".to_string(),
}
```

#### Expression

```rust
ActionCondition::Expression {
    expr: "severity == 'critical'".to_string(),
}
```

#### Logical Combinators

```rust
// AND
ActionCondition::And {
    conditions: vec![
        ActionCondition::FieldEquals {
            field: "severity".to_string(),
            value: serde_json::json!("critical"),
        },
        ActionCondition::FieldInRange {
            field: "error_count".to_string(),
            min: 10.0,
            max: f64::INFINITY,
        },
    ],
}

// OR
ActionCondition::Or {
    conditions: vec![
        ActionCondition::FieldEquals {
            field: "status".to_string(),
            value: serde_json::json!("error"),
        },
        ActionCondition::FieldEquals {
            field: "status".to_string(),
            value: serde_json::json!("critical"),
        },
    ],
}

// NOT
ActionCondition::Not {
    condition: Box::new(ActionCondition::FieldEquals {
        field: "environment".to_string(),
        value: serde_json::json!("production"),
    }),
}
```

## Dynamic Predicates

### Overview

Load predicates from external sources, enable A/B testing, and use feature flags per tenant.

### A/B Testing

```rust
use fuse_rule_core::rule_graph::dynamic_predicate::{
    DynamicPredicate, PredicateSource
};
use std::collections::HashMap;

let mut dynamic_pred = DynamicPredicate {
    rule_id: "price_alert".to_string(),
    source: PredicateSource::Static {
        predicate: "price > 100".to_string(),
    },
    versions: HashMap::new(),
    active_version: Some("v1".to_string()),
    feature_flags: HashMap::new(),
    refresh_interval_seconds: None,
};

// Add version A
dynamic_pred.add_version(
    "v1".to_string(),
    "price > 100".to_string(),
);

// Add version B (more aggressive)
dynamic_pred.add_version(
    "v2".to_string(),
    "price > 80".to_string(),
);

// Default users get v1
assert_eq!(
    dynamic_pred.get_predicate_for_tenant(None),
    Some("price > 100".to_string())
);

// Specific tenant gets v2 (beta test)
dynamic_pred.set_feature_flag("tenant_123".to_string(), "v2".to_string());
assert_eq!(
    dynamic_pred.get_predicate_for_tenant(Some("tenant_123")),
    Some("price > 80".to_string())
);
```

### Feature Flags Per Tenant

```rust
use fuse_rule_core::rule_graph::dynamic_predicate::PredicateLoader;

let loader = PredicateLoader::new();

// Register dynamic predicate with versions
loader.register(dynamic_pred).await;

// Get predicate for specific tenant
let predicate = loader.get_predicate("price_alert", Some("tenant_123")).await;
```

### External Predicate Sources

#### HTTP Source

```yaml
dynamic_predicates:
  - rule_id: "fraud_detection"
    source:
      type: "Http"
      url: "https://api.example.com/rules/fraud_detection/predicate"
      auth_header: "Bearer ${API_TOKEN}"
    refresh_interval_seconds: 300  # Reload every 5 minutes
```

#### Database Source

```yaml
dynamic_predicates:
  - rule_id: "compliance_check"
    source:
      type: "Database"
      connection_string: "postgresql://localhost/rules"
      table: "rule_predicates"
      predicate_column: "sql_expression"
      rule_id_column: "rule_id"
    refresh_interval_seconds: 600
```

#### S3 Source

```yaml
dynamic_predicates:
  - rule_id: "data_quality"
    source:
      type: "S3"
      bucket: "my-rules-bucket"
      key: "predicates/data_quality.sql"
      region: "us-east-1"
    refresh_interval_seconds: 1800
```

### Periodic Refresh

```rust
let loader = PredicateLoader::new();

// Start background refresh task (every 5 minutes)
loader.start_refresh_task(300);

// Predicates will be automatically reloaded
```

## Cross-Rule Correlation

### Overview

CEP-style temporal patterns across multiple rules:

- "Rule A AND Rule B within 5 minutes"
- "Rule A followed by Rule B within 10 seconds"
- "Rule A but NOT Rule B"

### Example: Security Alert Correlation

```text
Pattern: "login_failed AND suspicious_ip within 60 seconds"
         → Trigger account_lockout rule
```

### Usage

```rust
use fuse_rule_core::rule_graph::correlation::{
    CorrelationEngine, CorrelationPattern, TemporalWindow,
    WindowType, RuleActivationEvent
};
use chrono::Utc;
use std::collections::HashMap;

let mut engine = CorrelationEngine::new(1000);

// Define pattern: both rules must fire within 60 seconds
let pattern = CorrelationPattern::All {
    rule_ids: vec!["login_failed".to_string(), "suspicious_ip".to_string()],
    window: TemporalWindow {
        duration_seconds: 60,
        window_type: WindowType::Sliding,
    },
};

engine.add_pattern(pattern);

// Record activations
engine.record_activation(RuleActivationEvent {
    rule_id: "login_failed".to_string(),
    timestamp: Utc::now(),
    context: HashMap::new(),
});

engine.record_activation(RuleActivationEvent {
    rule_id: "suspicious_ip".to_string(),
    timestamp: Utc::now(),
    context: HashMap::new(),
});

// Check if pattern matches
let matching_patterns = engine.check_patterns();
if !matching_patterns.is_empty() {
    println!("🚨 Security alert triggered!");
}
```

### Pattern Types

#### ALL Pattern

All rules must fire within time window:

```rust
CorrelationPattern::All {
    rule_ids: vec!["A".to_string(), "B".to_string(), "C".to_string()],
    window: TemporalWindow {
        duration_seconds: 300,  // 5 minutes
        window_type: WindowType::Sliding,
    },
}
```

#### ANY Pattern

N out of M rules must fire:

```rust
CorrelationPattern::Any {
    rule_ids: vec![
        "metric_1".to_string(),
        "metric_2".to_string(),
        "metric_3".to_string(),
    ],
    count: 2,  // Any 2 of 3
    window: TemporalWindow {
        duration_seconds: 60,
        window_type: WindowType::Sliding,
    },
}
```

#### SEQUENCE Pattern

Rules must fire in order:

```rust
CorrelationPattern::Sequence {
    rule_ids: vec![
        "user_login".to_string(),
        "data_access".to_string(),
        "data_export".to_string(),
    ],
    window: TemporalWindow {
        duration_seconds: 600,  // 10 minutes
        window_type: WindowType::Sliding,
    },
}
```

#### NOT Pattern

Rule A but NOT Rule B:

```rust
CorrelationPattern::NotPattern {
    required_rule: "high_volume_trade".to_string(),
    excluded_rule: "approval_received".to_string(),
    window: TemporalWindow {
        duration_seconds: 120,
        window_type: WindowType::Sliding,
    },
}
```

## Complete Examples

### Example 1: E-commerce Fraud Detection Workflow

```rust
use fuse_rule_core::rule_graph::*;
use std::collections::HashMap;

// Step 1: Build rule chain
let mut chain = RuleChain::new();

chain.add_chain(ChainedRule {
    rule_id: "high_value_order".to_string(),
    downstream_rules: vec!["check_shipping_address".to_string()],
    trigger_conditions: HashMap::new(),
}).unwrap();

chain.add_chain(ChainedRule {
    rule_id: "check_shipping_address".to_string(),
    downstream_rules: vec!["fraud_score".to_string()],
    trigger_conditions: HashMap::new(),
}).unwrap();

chain.add_chain(ChainedRule {
    rule_id: "fraud_score".to_string(),
    downstream_rules: vec!["notify_fraud_team".to_string()],
    trigger_conditions: {
        let mut conditions = HashMap::new();
        conditions.insert(
            "notify_fraud_team".to_string(),
            TriggerCondition::Expression {
                expr: "fraud_score > 0.8".to_string(),
            },
        );
        conditions
    },
}).unwrap();

// Step 2: Add conditional actions to final rule
let mut action_set = ConditionalActionSet::new();

action_set.add_action(ConditionalAction {
    condition: ActionCondition::FieldInRange {
        field: "fraud_score".to_string(),
        min: 0.8,
        max: 0.95,
    },
    agent: "email_alert".to_string(),
    parameters: None,
    priority: 5,
});

action_set.add_action(ConditionalAction {
    condition: ActionCondition::FieldInRange {
        field: "fraud_score".to_string(),
        min: 0.95,
        max: 1.0,
    },
    agent: "block_transaction".to_string(),
    parameters: None,
    priority: 10,
});

// Step 3: Execute
let dag = RuleDAG::new(chain).unwrap();
let executor = DAGExecutor::new(dag);

let context = HashMap::new();
let triggered = executor.get_triggered_rules("high_value_order", &context);
println!("Triggered rules: {:?}", triggered);
```

### Example 2: SRE Alert Correlation

```rust
// Detect cascading failures: multiple services failing within 2 minutes
let mut engine = CorrelationEngine::new(1000);

engine.add_pattern(CorrelationPattern::Any {
    rule_ids: vec![
        "service_a_down".to_string(),
        "service_b_down".to_string(),
        "service_c_down".to_string(),
    ],
    count: 2,  // Any 2 services
    window: TemporalWindow {
        duration_seconds: 120,
        window_type: WindowType::Sliding,
    },
});

// Record service failures
engine.record_activation(RuleActivationEvent {
    rule_id: "service_a_down".to_string(),
    timestamp: Utc::now(),
    context: HashMap::new(),
});

engine.record_activation(RuleActivationEvent {
    rule_id: "service_b_down".to_string(),
    timestamp: Utc::now(),
    context: HashMap::new(),
});

// Pattern matches → cascading failure detected
if !engine.check_patterns().is_empty() {
    println!("🚨 Cascading failure detected!");
}
```

### Example 3: Multi-Tenant A/B Testing

```rust
// Different pricing rules for different customer segments
let mut dynamic_pred = DynamicPredicate {
    rule_id: "discount_eligibility".to_string(),
    source: PredicateSource::Static {
        predicate: "total_spend > 1000".to_string(),
    },
    versions: HashMap::new(),
    active_version: Some("control".to_string()),
    feature_flags: HashMap::new(),
    refresh_interval_seconds: None,
};

// Control group: conservative threshold
dynamic_pred.add_version(
    "control".to_string(),
    "total_spend > 1000".to_string(),
);

// Test group A: lower threshold
dynamic_pred.add_version(
    "variant_a".to_string(),
    "total_spend > 500".to_string(),
);

// Test group B: behavior-based
dynamic_pred.add_version(
    "variant_b".to_string(),
    "total_spend > 500 AND loyalty_score > 0.7".to_string(),
);

// Assign tenants to variants
dynamic_pred.set_feature_flag("tenant_100".to_string(), "control".to_string());
dynamic_pred.set_feature_flag("tenant_101".to_string(), "variant_a".to_string());
dynamic_pred.set_feature_flag("tenant_102".to_string(), "variant_b".to_string());

// Each tenant gets their own rule version
assert_eq!(
    dynamic_pred.get_predicate_for_tenant(Some("tenant_101")),
    Some("total_spend > 500".to_string())
);
```

## Performance Considerations

| Feature | Overhead | Notes |
|---------|----------|-------|
| Rule Chaining | ~0.5ms per hop | DAG validation done once at startup |
| Conditional Actions | ~0.1ms per action | Evaluated only on activation |
| Dynamic Predicates | ~1ms reload | Use appropriate refresh intervals |
| Correlation Patterns | ~2ms per check | Keep history size bounded |

## Best Practices

1. **Rule Chains**
   - Keep chains shallow (< 5 levels deep)
   - Use meaningful trigger conditions
   - Validate DAG on configuration changes

2. **Conditional Actions**
   - Order by priority (critical actions first)
   - Keep conditions simple and fast
   - Use expression evaluation sparingly

3. **Dynamic Predicates**
   - Cache predicates aggressively
   - Use reasonable refresh intervals (5-30 minutes)
   - Monitor predicate load failures

4. **Correlation Patterns**
   - Limit history size (< 1000 events per rule)
   - Use appropriate time windows
   - Clean up old activations periodically

## Troubleshooting

### "Cycle detected in rule chain"

You have a circular dependency. Use `topological_sort()` to identify the cycle:

```rust
match chain.topological_sort() {
    Ok(order) => println!("Valid DAG: {:?}", order),
    Err(e) => println!("Cycle detected: {}", e),
}
```

### "Conditional action not firing"

Debug the evaluation:

```rust
let mut context = HashMap::new();
context.insert("severity".to_string(), serde_json::json!("critical"));

println!("Condition result: {}", condition.evaluate(&context));
```

### "Correlation pattern never matches"

Check time windows and activation history:

```rust
println!("Pattern check: {:?}", engine.check_patterns());
println!("History: {:?}", engine.activation_history);
```

---

**Next Steps**: See [OBSERVABILITY.md](OBSERVABILITY.md) for monitoring rule workflows in production
