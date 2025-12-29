use arrow_rule_agent::RuleEngine;
use arrow_rule_agent::rule::Rule;
use arrow_rule_agent::agent::{Agent, LoggerAgent};
use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, Int32Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use anyhow::Result;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    println!("🚀 Starting V3 Windowed Rules Demo");
    
    let temp_dir = tempfile::tempdir()?;
    let mut engine = RuleEngine::new(temp_dir.path())?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Rule: Trigger if the average value > 50 over a 3-second window
    engine.add_rule(Rule {
        id: "w1".to_string(),
        name: "High Temp Alert (Windowed)".to_string(),
        predicate: "AVG(value) > 50".to_string(), // DataFusion handles aggregates
        action: "cool_down".to_string(),
        window_seconds: Some(3),
    }, &schema)?;

    let logger = LoggerAgent;

    println!("\n📥 T=0s: Normal values (Value=20)");
    let b1 = create_batch(&schema, "S1", 20)?;
    process(&mut engine, &b1, &logger).await?;

    println!("\n📥 T=1s: Spike (Value=100) -> Avg should be (20+100)/2 = 60 (> 50)");
    let b2 = create_batch(&schema, "S1", 100)?;
    process(&mut engine, &b2, &logger).await?;

    println!("\n📥 T=2s: Values cool down (Value=10) -> Avg should be (20+100+10)/3 = 43 (< 50)");
    let b3 = create_batch(&schema, "S1", 10)?;
    process(&mut engine, &b3, &logger).await?;

    println!("\n⏳ Waiting 4 seconds for window to clear...");
    sleep(Duration::from_secs(4)).await;

    println!("\n📥 T=6s: Another spike (Value=100) -> Avg should be 100 (> 50) -> Should re-trigger!");
    let b4 = create_batch(&schema, "S1", 100)?;
    process(&mut engine, &b4, &logger).await?;

    println!("\n✅ Demo Completed");
    Ok(())
}

fn create_batch(schema: &Arc<Schema>, id: &str, val: i32) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(Int32Array::from(vec![val])),
        ],
    ).map_err(Into::into)
}

async fn process(engine: &mut RuleEngine, batch: &RecordBatch, agent: &dyn Agent) -> Result<()> {
    let activations = engine.process_batch(batch).await?;
    if activations.is_empty() {
        println!("  (Criteria not met / No transition)");
    }
    for act in activations {
        agent.execute(&act);
    }
    Ok(())
}
