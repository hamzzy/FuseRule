use anyhow::Result;
use fuse_rule_core::rule_graph::chain::{ChainedRule, RuleChain, TriggerCondition};
use fuse_rule_core::rule_graph::dag::{DAGExecutor, RuleDAG};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🔀 FuseRule DAG Execution Example\n");
    println!("This example demonstrates rule execution as a Directed Acyclic Graph");
    println!("with topological ordering and conditional triggering\n");

    // Example 1: Simple Chain (A -> B -> C)
    println!("=" .repeat(60));
    println!("Example 1: Simple Linear Chain");
    println!("=" .repeat(60));

    let mut chain = RuleChain::new();

    chain.add_chain(ChainedRule {
        rule_id: "validate_input".to_string(),
        downstream_rules: vec!["process_data".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    chain.add_chain(ChainedRule {
        rule_id: "process_data".to_string(),
        downstream_rules: vec!["send_notification".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    chain.add_chain(ChainedRule {
        rule_id: "send_notification".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    let dag = RuleDAG::new(chain.clone())?;
    let plan = dag.get_execution_plan()?;

    println!("\n📊 Execution Plan:");
    println!("  Order: {:?}", plan.execution_order);
    println!("\n  Levels:");
    for (rule_id, level) in &plan.levels {
        println!("    Level {}: {}", level, rule_id);
    }

    // Example 2: Diamond Pattern (A -> B,C -> D)
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 2: Diamond Pattern with Parallel Execution");
    println!("=" .repeat(60));

    let mut chain2 = RuleChain::new();

    chain2.add_chain(ChainedRule {
        rule_id: "ingest".to_string(),
        downstream_rules: vec!["validate".to_string(), "enrich".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    chain2.add_chain(ChainedRule {
        rule_id: "validate".to_string(),
        downstream_rules: vec!["aggregate".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    chain2.add_chain(ChainedRule {
        rule_id: "enrich".to_string(),
        downstream_rules: vec!["aggregate".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    chain2.add_chain(ChainedRule {
        rule_id: "aggregate".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    let dag2 = RuleDAG::new(chain2)?;
    let batches = dag2.get_parallel_batches()?;

    println!("\n📊 Parallel Execution Batches:");
    for (i, batch) in batches.iter().enumerate() {
        println!("  Batch {}: {:?}", i, batch);
        if batch.len() > 1 {
            println!("    ⚡ Can execute in parallel!");
        }
    }

    // Example 3: Conditional Triggering
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 3: Conditional Triggering");
    println!("=" .repeat(60));

    let mut chain3 = RuleChain::new();
    let mut trigger_conditions = HashMap::new();

    // Only trigger if status == 'success'
    trigger_conditions.insert(
        "process_success".to_string(),
        TriggerCondition::FieldEquals {
            field: "status".to_string(),
            value: "success".to_string(),
        },
    );

    trigger_conditions.insert(
        "process_failure".to_string(),
        TriggerCondition::FieldEquals {
            field: "status".to_string(),
            value: "failure".to_string(),
        },
    );

    chain3.add_chain(ChainedRule {
        rule_id: "check_status".to_string(),
        downstream_rules: vec!["process_success".to_string(), "process_failure".to_string()],
        trigger_conditions: trigger_conditions.clone(),
    })?;

    chain3.add_chain(ChainedRule {
        rule_id: "process_success".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    chain3.add_chain(ChainedRule {
        rule_id: "process_failure".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    let dag3 = RuleDAG::new(chain3)?;
    let executor = DAGExecutor::new(dag3);

    println!("\n🔍 Testing Conditional Triggers:");

    // Test with success status
    let mut context_success = HashMap::new();
    context_success.insert("status".to_string(), serde_json::json!("success"));

    let triggered = executor.get_triggered_rules("check_status", &context_success);
    println!("\n  Context: status = 'success'");
    println!("  Triggered Rules: {:?}", triggered);

    // Test with failure status
    let mut context_failure = HashMap::new();
    context_failure.insert("status".to_string(), serde_json::json!("failure"));

    let triggered = executor.get_triggered_rules("check_status", &context_failure);
    println!("\n  Context: status = 'failure'");
    println!("  Triggered Rules: {:?}", triggered);

    // Example 4: Expression-Based Triggering
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 4: Expression-Based Triggering");
    println!("=" .repeat(60));

    let mut chain4 = RuleChain::new();
    let mut expr_conditions = HashMap::new();

    expr_conditions.insert(
        "high_priority".to_string(),
        TriggerCondition::Expression {
            expr: "priority > 5".to_string(),
        },
    );

    expr_conditions.insert(
        "low_priority".to_string(),
        TriggerCondition::Expression {
            expr: "priority <= 5".to_string(),
        },
    );

    chain4.add_chain(ChainedRule {
        rule_id: "classify".to_string(),
        downstream_rules: vec!["high_priority".to_string(), "low_priority".to_string()],
        trigger_conditions: expr_conditions,
    })?;

    chain4.add_chain(ChainedRule {
        rule_id: "high_priority".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    chain4.add_chain(ChainedRule {
        rule_id: "low_priority".to_string(),
        downstream_rules: vec![],
        trigger_conditions: HashMap::new(),
    })?;

    let dag4 = RuleDAG::new(chain4)?;
    let executor4 = DAGExecutor::new(dag4);

    println!("\n🔍 Testing Expression Triggers:");

    let mut context_high = HashMap::new();
    context_high.insert("priority".to_string(), serde_json::json!(8));

    let triggered = executor4.get_triggered_rules("classify", &context_high);
    println!("\n  Context: priority = 8");
    println!("  Triggered Rules: {:?}", triggered);

    let mut context_low = HashMap::new();
    context_low.insert("priority".to_string(), serde_json::json!(3));

    let triggered = executor4.get_triggered_rules("classify", &context_low);
    println!("\n  Context: priority = 3");
    println!("  Triggered Rules: {:?}", triggered);

    // Example 5: Consecutive Count Tracking
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 5: Consecutive Count Tracking");
    println!("=" .repeat(60));

    let mut chain5 = RuleChain::new();
    chain5.add_chain(ChainedRule {
        rule_id: "monitor".to_string(),
        downstream_rules: vec!["alert".to_string()],
        trigger_conditions: HashMap::new(),
    })?;

    let dag5 = RuleDAG::new(chain5)?;
    let executor5 = DAGExecutor::new(dag5);

    println!("\n🔢 Tracking Consecutive Activations:");

    let key = "monitor->alert";
    for i in 1..=5 {
        let count = executor5.increment_consecutive_count(key).await;
        println!("  Activation {}: count = {}", i, count);
        
        if count >= 3 {
            println!("    ⚠️  Threshold reached! Trigger alert.");
        }
    }

    println!("\n  Resetting count...");
    executor5.reset_consecutive_count(key).await;
    let count = executor5.get_consecutive_count(key).await;
    println!("  Count after reset: {}", count);

    // Summary
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Summary");
    println!("=" .repeat(60));
    println!("✅ Demonstrated Features:");
    println!("  - Topological ordering of rule execution");
    println!("  - Parallel execution batches");
    println!("  - Conditional triggering based on field values");
    println!("  - Expression-based triggering");
    println!("  - Consecutive count tracking");
    println!("  - Entry point and leaf rule identification");

    println!("\n💡 Use Cases:");
    println!("  - Complex multi-stage data pipelines");
    println!("  - Conditional workflow execution");
    println!("  - Parallel processing optimization");
    println!("  - Alert escalation based on frequency");
    println!("  - Dynamic rule chaining");

    println!("\n✨ DAG Execution Example Complete!\n");

    Ok(())
}
