//! Programmatic rule management example
//!
//! This example shows how to:
//! 1. Create an engine programmatically
//! 2. Add rules dynamically
//! 3. Update and toggle rules

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_rule_agent::{
    evaluator::DataFusionEvaluator, rule::Rule, state::SledStateStore, RuleEngine,
};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("🔥 FuseRule Programmatic Rules Example\n");

    // Create temporary directory for state
    let temp_dir = TempDir::new()?;
    let state_path = temp_dir.path().join("state");

    // 1. Create schema
    let schema = Schema::new(vec![Field::new("price", DataType::Float64, true)]);
    let schema = Arc::new(schema);

    // 2. Create engine components
    let evaluator = Box::new(DataFusionEvaluator::new());
    let state = Box::new(SledStateStore::new(&state_path)?);

    // 3. Create engine
    let mut engine = RuleEngine::new(evaluator, state, schema, 1000, 10);

    // 4. Add a rule programmatically
    println!("📝 Adding rule programmatically...");
    let rule = Rule {
        id: "high_price".to_string(),
        name: "High Price Alert".to_string(),
        predicate: "price > 1000".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
    };
    engine.add_rule(rule).await?;
    println!("   ✅ Rule added");

    // 5. Create test batch
    let batch = RecordBatch::try_new(
        engine.schema(),
        vec![Arc::new(Float64Array::from(vec![1500.0, 500.0]))],
    )?;

    // 6. Process batch
    println!("🔍 Processing batch...");
    let traces = engine.process_batch(&batch).await?;
    println!("   ✅ Processed {} rules", traces.len());

    // 7. Update the rule
    println!("🔄 Updating rule...");
    let updated_rule = Rule {
        id: "high_price".to_string(),
        name: "High Price Alert (Updated)".to_string(),
        predicate: "price > 2000".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 2,
        enabled: true,
    };
    engine.update_rule("high_price", updated_rule).await?;
    println!("   ✅ Rule updated");

    // 8. Toggle rule
    println!("🔀 Disabling rule...");
    engine.toggle_rule("high_price", false).await?;
    println!("   ✅ Rule disabled");

    // 9. Process again (rule should not fire)
    let _traces = engine.process_batch(&batch).await?;
    println!("   ✅ Processed (rule disabled, should not fire)");

    Ok(())
}
