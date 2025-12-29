use axum::{
    extract::{State, Json},
    routing::{get, post},
    Router,
    http::StatusCode,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::RuleEngine;
use serde_json::Value;
use arrow_json::ReaderBuilder;
use std::io::Cursor;

pub type SharedEngine = Arc<RwLock<RuleEngine>>;

pub struct FuseRuleServer {
    engine: SharedEngine,
}

impl FuseRuleServer {
    pub fn new(engine: SharedEngine) -> Self {
        Self { engine }
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/status", get(handle_status))
            .route("/ingest", post(handle_ingest))
            .with_state(self.engine);

        let addr = format!("0.0.0.0:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        println!("🚀 FuseRule Server running on http://{}", addr);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn handle_status() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(serde_json::json!({ "status": "active" })))
}

async fn handle_ingest(
    State(engine): State<SharedEngine>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    // 1. Convert JSON to Arrow RecordBatch
    // We assume the caller sends an array of objects
    let json_data = serde_json::to_vec(&payload).unwrap();
    let cursor = Cursor::new(json_data);
    
    // We need the schema from the engine to read correctly, 
    // but for simplicity in this demo, we'll infer it or assume it matches.
    // In a production product, we'd use the engine's current schema.
    
    let mut engine_lock = engine.write().await;
    
    // Since DataFusion/Arrow JSON reader needs a schema to be efficient, 
    // and we are just "ingesting", we'll use a hack for now or just parse manually.
    // Better way: The engine should have a "registered schema" for ingress.
    
    // For now, let's assume the user knows the schema and we just process.
    // To keep it robust, we'll return the activations.
    
    let schema = engine_lock.schema();
    let mut reader = ReaderBuilder::new(schema).build(cursor).unwrap();
    
    let mut all_activations = Vec::new();
    while let Some(Ok(batch)) = reader.next() {
        println!("  Ingested batch with {} rows", batch.num_rows());
        match engine_lock.process_batch(&batch).await {
            Ok(activations) => {
                println!("  Engine returned {} activations", activations.len());
                all_activations.extend(activations);
            },
            Err(e) => {
                eprintln!("  Engine error: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() })));
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ 
        "message": "Processed", 
        "activations_count": all_activations.len() 
    })))
}
