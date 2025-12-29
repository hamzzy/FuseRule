use anyhow::Result;
use arrow::array::{Float64Array, Int32Array};
use arrow::record_batch::RecordBatch;
use arrow_rule_agent::config::FuseRuleConfig;
use arrow_rule_agent::RuleEngine;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    println!("🚀 Starting FuseRule V4: Dynamic Config & Webhook Demo");

    // 1. Start a mock webhook server in the background
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
        println!("🛰️ Mock Webhook Server listening on http://127.0.0.1:8080/webhook");
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0; 1024];
                let n = socket.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]);
                if request.contains("POST") {
                    println!("\n🔔 [Mock Server] Received Webhook Notification!");
                    let body = request.split("\r\n\r\n").last().unwrap_or("");
                    println!("📦 Payload: {}", body);
                    // Send 200 OK
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                }
            });
        }
    });

    // 2. Load Config from YAML
    let config = FuseRuleConfig::from_file("fuse_rule_config.yaml")?;
    println!(
        "📖 Config loaded: {} rules, {} agents",
        config.rules.len(),
        config.agents.len()
    );

    // 3. Initialize Engine (Rules and Agents are loaded here)
    let mut engine = RuleEngine::from_config(config.clone()).await?;

    // 4. Setup Data Schema (for manual batch creation)
    let schema = engine.schema();

    // 5. Simulate Data
    println!("\n📥 Batch 1: High CPU usage (r2: System Spike)");
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![500, 200])),
            Arc::new(Float64Array::from(vec![95.0, 92.0])),
        ],
    )?;
    engine.process_batch(&batch1).await?;

    println!("\n📥 Batch 2: High Amount (r1: High Value Transaction)");
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1500, 10])),
            Arc::new(Float64Array::from(vec![40.0, 10.0])),
        ],
    )?;
    engine.process_batch(&batch2).await?;

    // Wait a bit for async agents to finish
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    println!("\n✅ V4 Demo Completed");
    Ok(())
}
