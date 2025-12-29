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
    println!("🚀 Starting Arrow Rule Agent Demo - Phase 1: Core Engine");

    let mut engine = RuleEngine::new()?;

    // 1. Setup Data Schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("severity", DataType::Utf8, false),
        Field::new("count", DataType::Int32, false),
    ]));

    // 2. Define Rules
    engine.add_rule(Rule {
        id: "r1".to_string(),
        name: "High Severity Alert".to_string(),
        predicate: "severity = 'CRITICAL' AND count > 5".to_string(),
        action: "notify_slack".to_string(),
    }, &schema)?;

    engine.add_rule(Rule {
        id: "r2".to_string(),
        name: "Burst Detection".to_string(),
        predicate: "count > 100".to_string(),
        action: "scale_up".to_string(),
    }, &schema)?;

    // 3. Setup Agents
    let logger = LoggerAgent;

    // 4. Simulate Data Stream
    println!("\n📥 Batch 1: Low-traffic logs");
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["INFO", "WARN"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )?;
    process_and_trigger(&mut engine, &batch1, &logger).await?;

    println!("\n📥 Batch 2: Critical error appears, but count is low");
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["CRITICAL", "INFO"])),
            Arc::new(Int32Array::from(vec![2, 10])),
        ],
    )?;
    process_and_trigger(&mut engine, &batch2, &logger).await?;

    println!("\n📥 Batch 3: High Severity threshold met! (Severity=CRITICAL, Count=10)");
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["CRITICAL", "DEBUG"])),
            Arc::new(Int32Array::from(vec![10, 5])),
        ],
    )?;
    process_and_trigger(&mut engine, &batch3, &logger).await?;

    println!("\n📥 Batch 4: Same critical state (Should NOT fire again - Edge-Triggered)");
    let batch4 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["CRITICAL", "INFO"])),
            Arc::new(Int32Array::from(vec![15, 2])),
        ],
    )?;
    process_and_trigger(&mut engine, &batch4, &logger).await?;

    println!("\n✅ Demo Completed");
    Ok(())
}

async fn process_and_trigger(engine: &mut RuleEngine, batch: &RecordBatch, agent: &dyn Agent) -> Result<()> {
    let activations = engine.process_batch(batch).await?;
    
    if activations.is_empty() {
        println!("  (No new transitions)");
    }

    for activation in activations {
        agent.execute(&activation);
    }
    Ok(())
}
