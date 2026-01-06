//! Basic usage example for FuseRule
//!
//! This example shows how to:
//! 1. Create a rule engine from configuration
//! 2. Process data batches
//! 3. Inspect evaluation traces

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_rule_core::{config::FuseRuleConfig, RuleEngine};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🔥 FuseRule Basic Usage Example\n");

    // 1. Load configuration
    println!("📝 Loading configuration...");
    let config = FuseRuleConfig::from_file("fuse_rule_config.yaml")?;
    println!("   ✅ Loaded {} rules", config.rules.len());

    // 2. Create engine from config
    println!("⚙️  Creating rule engine...");
    let mut engine = RuleEngine::from_config(config).await?;
    println!("   ✅ Engine created");

    // 3. Create a test batch
    println!("📦 Creating test batch...");
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("symbol", DataType::Utf8, true),
    ]);

    let price_array = Arc::new(Float64Array::from(vec![1500.0, 500.0, 2000.0]));
    let symbol_array = Arc::new(StringArray::from(vec!["AAPL", "GOOGL", "MSFT"]));

    let batch = RecordBatch::try_new(Arc::new(schema), vec![price_array, symbol_array])?;
    println!("   ✅ Created batch with {} rows", batch.num_rows());

    // 4. Process batch and get evaluation traces
    println!("🔍 Processing batch and evaluating rules...\n");
    let traces = engine.process_batch(&batch).await?;

    // 5. Display results
    println!("📊 Evaluation Results:");
    println!("{}", "=".repeat(60));
    for trace in traces {
        let status = if trace.action_fired {
            "🔥 FIRED"
        } else {
            "⚪"
        };
        println!("{} Rule: {} ({})", status, trace.rule_name, trace.rule_id);
        println!("   Result: {:?}", trace.result);
        println!("   Transition: {}", trace.transition);
        if let Some(agent_status) = trace.agent_status {
            println!("   Agent Status: {}", agent_status);
        }
        println!();
    }

    Ok(())
}
