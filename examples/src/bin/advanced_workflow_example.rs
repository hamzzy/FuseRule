//! Comprehensive example demonstrating all advanced rule graph features
//!
//! This example shows:
//! 1. DAG execution - Rules triggering other rules in a workflow
//! 2. Conditional actions - Multiple actions per rule based on runtime context
//! 3. Cross-rule correlation - Temporal pattern matching
//! 4. Dynamic predicates - Tenant-specific rule conditions
//!
//! Example scenario: Trading alert system with escalation workflow

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_rule_core::{
    agent::{Agent, Activation},
    config::FuseRuleConfig,
    RuleEngine,
};
use std::sync::Arc;
use tokio;

/// Simple logger agent for demonstration
#[derive(Clone)]
struct LoggerAgent {
    name: String,
}

#[async_trait::async_trait]
impl Agent for LoggerAgent {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, activation: &Activation) -> anyhow::Result<()> {
        println!(
            "📢 [{}] Rule '{}' activated!",
            self.name, activation.rule_name
        );
        if let Some(batch) = &activation.context {
            println!("   └─ Matched {} rows", batch.num_rows());
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🚀 FuseRule Advanced Workflow Example");
    println!("=====================================\n");

    // Create configuration with all advanced features
    let config_yaml = r#"
engine:
  persistence_path: "/tmp/fuse_rule_advanced"
  max_pending_batches: 1000
  agent_concurrency: 10

schema:
  - name: "price"
    data_type: "float64"
  - name: "volume"
    data_type: "int64"
  - name: "severity"
    data_type: "utf8"
  - name: "market"
    data_type: "utf8"

agents:
  - name: "logger"
    type: "logger"
  - name: "slack"
    type: "logger"
  - name: "pagerduty"
    type: "logger"
  - name: "alert_trader"
    type: "logger"
  - name: "alert_risk"
    type: "logger"

rules:
  # Entry point: Detect high price
  - id: "high_price"
    name: "High Price Detection"
    predicate: "price > 100"
    action: "logger"
    actions:
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
    downstream_rules:
      - rule_id: "check_volume"
        condition:
          type: "Always"
      - rule_id: "check_volatility"
        condition:
          type: "FieldEquals"
          field: "market"
          value: "crypto"

  # Check volume (triggered by high_price)
  - id: "check_volume"
    name: "Volume Verification"
    predicate: "volume > 1000"
    action: "logger"
    downstream_rules:
      - rule_id: "alert_trader"
        condition:
          type: "Expression"
          expr: "volume > 5000"

  # Check volatility (triggered conditionally)
  - id: "check_volatility"
    name: "Volatility Check"
    predicate: "price > 150"
    action: "alert_risk"

  # Terminal rule: Alert trader
  - id: "alert_trader"
    name: "Alert Trader"
    predicate: "price > 0"
    action: "alert_trader"

  # Meta-rule for correlation pattern
  - id: "complex_pattern"
    name: "Complex Trading Pattern Detected"
    predicate: "price > 0"
    action: "pagerduty"

# Correlation patterns
correlation_patterns:
  - type: "Sequence"
    rule_ids: ["high_price", "check_volume", "alert_trader"]
    window:
      duration_seconds: 300
      window_type: "Sliding"
    meta_rule_id: "complex_pattern"
"#;

    // Parse configuration
    let config: FuseRuleConfig = serde_yaml::from_str(config_yaml)?;
    println!("✅ Configuration loaded");
    println!("   ├─ {} rules defined", config.rules.len());
    println!("   ├─ {} agents configured", config.agents.len());
    println!("   └─ {} correlation patterns", config.correlation_patterns.len());
    println!();

    // Create engine
    let mut engine = RuleEngine::from_config(config).await?;

    // Add agents (in real scenario, these would be actual integrations)
    engine.add_agent("logger".to_string(), Arc::new(LoggerAgent { name: "Logger".to_string() }));
    engine.add_agent("slack".to_string(), Arc::new(LoggerAgent { name: "Slack".to_string() }));
    engine.add_agent("pagerduty".to_string(), Arc::new(LoggerAgent { name: "PagerDuty".to_string() }));
    engine.add_agent("alert_trader".to_string(), Arc::new(LoggerAgent { name: "TraderAlert".to_string() }));
    engine.add_agent("alert_risk".to_string(), Arc::new(LoggerAgent { name: "RiskAlert".to_string() }));

    println!("✅ Engine initialized with advanced features:");
    if engine.dag_executor.is_some() {
        println!("   ├─ ✓ DAG execution enabled");
    }
    if engine.correlation_engine.is_some() {
        println!("   ├─ ✓ Correlation engine active");
    }
    if !engine.conditional_actions.is_empty() {
        println!("   ├─ ✓ Conditional actions configured");
    }
    if engine.predicate_loader.is_some() {
        println!("   └─ ✓ Dynamic predicates ready");
    }
    println!();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int64, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("market", DataType::Utf8, true),
    ]));

    println!("📊 Test Scenario 1: High price with critical severity (crypto market)");
    println!("─────────────────────────────────────────────────────────────────");

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![150.0])),
            Arc::new(Int64Array::from(vec![6000])),
            Arc::new(StringArray::from(vec!["critical"])),
            Arc::new(StringArray::from(vec!["crypto"])),
        ],
    )?;

    println!("Input: price=150, volume=6000, severity=critical, market=crypto\n");
    let traces = engine.process_batch(&batch1).await?;

    println!("\n📝 Execution Flow:");
    for (i, trace) in traces.iter().enumerate() {
        if trace.action_fired {
            println!("   {}. {} → {} ({})",
                i + 1,
                trace.rule_name,
                trace.transition,
                trace.agent_status.as_ref().unwrap_or(&"unknown".to_string())
            );
        }
    }

    println!("\n Expected DAG cascade:");
    println!("   high_price (entry)");
    println!("   ├─→ check_volume (always triggered)");
    println!("   │   └─→ alert_trader (volume > 5000)");
    println!("   └─→ check_volatility (market == crypto)");
    println!();

    // Small delay to let async operations complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n📊 Test Scenario 2: High price with warning severity");
    println!("───────────────────────────────────────────────────");

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![120.0])),
            Arc::new(Int64Array::from(vec![2000])),
            Arc::new(StringArray::from(vec!["warning"])),
            Arc::new(StringArray::from(vec!["stocks"])),
        ],
    )?;

    println!("Input: price=120, volume=2000, severity=warning, market=stocks\n");
    let traces = engine.process_batch(&batch2).await?;

    println!("\n📝 Execution Flow:");
    for (i, trace) in traces.iter().enumerate() {
        if trace.action_fired {
            println!("   {}. {} → {}",
                i + 1,
                trace.rule_name,
                trace.transition
            );
        }
    }

    println!("\n Expected behavior:");
    println!("   - Conditional action: Slack (not PagerDuty)");
    println!("   - DAG: check_volume triggered");
    println!("   - DAG: check_volatility NOT triggered (market != crypto)");
    println!();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n📊 Test Scenario 3: Trigger correlation pattern");
    println!("──────────────────────────────────────────────────");

    // This batch should complete the sequence pattern
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![160.0])),
            Arc::new(Int64Array::from(vec![8000])),
            Arc::new(StringArray::from(vec!["critical"])),
            Arc::new(StringArray::from(vec!["crypto"])),
        ],
    )?;

    println!("Input: price=160, volume=8000, severity=critical, market=crypto\n");
    let _traces = engine.process_batch(&batch3).await?;

    println!("Expected: Correlation pattern 'Sequence' may match");
    println!("   (high_price → check_volume → alert_trader)");
    println!("   → Should trigger meta-rule: 'Complex Trading Pattern Detected'\n");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n✅ Advanced Workflow Example Complete!");
    println!("\n🎯 Features Demonstrated:");
    println!("   ✓ DAG Execution - Rules triggered downstream in workflow");
    println!("   ✓ Conditional Actions - Different agents based on severity");
    println!("   ✓ Trigger Conditions - Conditional cascading (market type)");
    println!("   ✓ Expression Conditions - Volume threshold for trader alerts");
    println!("   ✓ Correlation Patterns - Sequential pattern detection");
    println!("   ✓ Meta-Rules - Triggered on pattern matches");
    println!("\n💡 This demonstrates how FuseRule transforms from simple alerts");
    println!("   into expressive decision workflows with complex orchestration!");

    Ok(())
}
