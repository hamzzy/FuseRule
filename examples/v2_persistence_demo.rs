use arrow_rule_agent::RuleEngine;
use arrow_rule_agent::rule::Rule;
use arrow_rule_agent::agent::{Agent, LoggerAgent};
use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, Int32Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let db_path = "v2_demo_state";
    
    // Clean up previous run
    let _ = std::fs::remove_dir_all(db_path);

    println!("🚀 Starting V2 Persistence Demo");
    println!("1. Initializing engine with path: {}", db_path);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("severity", DataType::Utf8, false),
        Field::new("count", DataType::Int32, false),
    ]));

    let rule = Rule {
        id: "p1".to_string(),
        name: "Persistence Test".to_string(),
        predicate: "count > 10".to_string(),
        action: "restart_alert".to_string(),
    };

    {
        println!("\n--- Process 1: Triggering Rule ---");
        let mut engine = RuleEngine::new(db_path)?;
        engine.add_rule(rule.clone(), &schema)?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["INFO"])),
                Arc::new(Int32Array::from(vec![50])),
            ],
        )?;

        let activations = engine.process_batch(&batch).await?;
        for act in activations {
            LoggerAgent.execute(&act);
        }
        println!("Process 1 finished.");
    }

    println!("\n--- Simulating Restart (New Engine Instance) ---");

    {
        println!("\n--- Process 2: Checking if Rule Refires ---");
        let mut engine = RuleEngine::new(db_path)?;
        engine.add_rule(rule.clone(), &schema)?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["INFO"])),
                Arc::new(Int32Array::from(vec![60])),
            ],
        )?;

        let activations = engine.process_batch(&batch).await?;
        if activations.is_empty() {
            println!("✅ Success: Rule did NOT refire after restart (Deterministic Persistence)");
        } else {
            println!("❌ Failure: Rule refired!");
        }
    }

    // Clean up
    let _ = std::fs::remove_dir_all(db_path);
    Ok(())
}
