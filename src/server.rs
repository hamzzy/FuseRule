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
            .route("/metrics", get(handle_metrics))
            .route("/ingest", post(handle_ingest))
            .with_state(self.engine);

        let addr = format!("0.0.0.0:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        println!("🚀 FuseRule Server running on http://{}", addr);
        
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        
        println!("🛑 FuseRule Server shut down gracefully");
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn handle_status() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(serde_json::json!({ "status": "active" })))
}

async fn handle_metrics() -> (StatusCode, Json<Value>) {
    let snapshot = crate::metrics::METRICS.snapshot();
    (StatusCode::OK, Json(serde_json::to_value(&snapshot).unwrap()))
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
    let reader = ReaderBuilder::new(schema).build(cursor).unwrap();
    
    let mut all_traces = Vec::new();
    let iter = reader.into_iter();
    for batch_result in iter {
        match batch_result {
            Ok(batch) => {
                println!("  Ingested batch with {} rows", batch.num_rows());
                match engine_lock.process_batch(&batch).await {
                    Ok(traces) => {
                        println!("  Engine processed batch: {} rules evaluated", traces.len());
                        all_traces.extend(traces);
                    },
                    Err(e) => {
                        eprintln!("  Engine error: {}", e);
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() })));
                    }
                }
            },
            Err(e) => {
                eprintln!("  Reader error: {}", e);
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": format!("JSON Reader error: {}", e) })));
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ 
        "message": "Processed", 
        "traces": all_traces 
    })))
}
