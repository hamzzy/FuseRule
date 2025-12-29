use crate::RuleEngine;
use crate::evaluator::RuleEvaluator;
use arrow_json::ReaderBuilder;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub type SharedEngine = Arc<RwLock<RuleEngine>>;

/// Simple token bucket rate limiter
struct RateLimiter {
    tokens: Arc<Mutex<u32>>,
    max_tokens: u32,
    refill_interval: Duration,
    last_refill: Arc<Mutex<Instant>>,
}

impl RateLimiter {
    fn new(requests_per_second: u32) -> Self {
        Self {
            tokens: Arc::new(Mutex::new(requests_per_second)),
            max_tokens: requests_per_second,
            refill_interval: Duration::from_secs(1),
            last_refill: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn allow(&self) -> bool {
        let mut tokens = self.tokens.lock().await;
        let mut last_refill = self.last_refill.lock().await;
        
        // Refill tokens based on elapsed time
        let elapsed = last_refill.elapsed();
        if elapsed >= self.refill_interval {
            let refills = (elapsed.as_secs_f64() / self.refill_interval.as_secs_f64()) as u32;
            *tokens = (*tokens + refills).min(self.max_tokens);
            *last_refill = Instant::now();
        }
        
        // Consume a token if available
        if *tokens > 0 {
            *tokens -= 1;
            true
        } else {
            false
        }
    }
}

pub struct FuseRuleServer {
    engine: SharedEngine,
    config_path: String,
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl FuseRuleServer {
    pub fn new(engine: SharedEngine, config_path: String, rate_limit: Option<u32>) -> Self {
        let rate_limiter = rate_limit.map(|rps| Arc::new(RateLimiter::new(rps)));
        Self {
            engine,
            config_path,
            rate_limiter,
        }
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let rate_limiter = self.rate_limiter.clone();
        let app = Router::new()
            .route("/status", get(handle_status))
            .route("/health", get(handle_health))
            .route("/metrics", get(handle_metrics))
            .route("/rules", get(handle_rules))
            .route("/api/v1/rules", post(handle_create_rule))
            .route("/api/v1/rules/:rule_id", axum::routing::put(handle_update_rule))
            .route("/api/v1/rules/:rule_id", axum::routing::delete(handle_delete_rule))
            .route("/api/v1/state", get(handle_state))
            .route("/api/v1/state/:rule_id", get(handle_rule_state))
            .route("/ingest", post(move |state, body| {
                handle_ingest_with_rate_limit(state, body, rate_limiter.clone())
            }))
            .with_state(self.engine.clone());

        let addr = format!("0.0.0.0:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("FuseRule Server running on http://{}", addr);

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(
                self.engine.clone(),
                self.config_path.clone(),
            ))
            .await?;

        info!("FuseRule Server shut down gracefully");
        Ok(())
    }
}

// Dynamic Rule Management API

#[derive(serde::Deserialize)]
struct CreateRuleRequest {
    id: String,
    name: String,
    predicate: String,
    action: String,
    window_seconds: Option<u64>,
    version: Option<u32>,
    enabled: Option<bool>,
    #[serde(default)]
    dry_run: bool,
}

async fn handle_create_rule(
    State(engine): State<SharedEngine>,
    Json(req): Json<CreateRuleRequest>,
) -> impl IntoResponse {
    use crate::rule::Rule;
    
    let rule = Rule {
        id: req.id.clone(),
        name: req.name.clone(),
        predicate: req.predicate.clone(),
        action: req.action.clone(),
        window_seconds: req.window_seconds,
        version: req.version.unwrap_or(1),
        enabled: req.enabled.unwrap_or(true),
    };
    
    // Validate rule by attempting to compile it
    let engine_lock = engine.read().await;
    let schema = engine_lock.schema();
    let evaluator = crate::evaluator::DataFusionEvaluator::new();
    
    match evaluator.compile(rule.clone(), &schema) {
        Ok(_) => {
            if req.dry_run {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "message": "Rule validation successful",
                        "rule": rule,
                        "dry_run": true
                    })),
                );
            }
        }
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Rule compilation failed",
                    "message": e.to_string()
                })),
            );
        }
    }
    
    drop(engine_lock);
    
    // Add rule to engine
    let mut engine_lock = engine.write().await;
    match engine_lock.add_rule(rule.clone()).await {
        Ok(()) => {
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "message": "Rule created successfully",
                    "rule": rule
                })),
            )
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to add rule",
                    "message": e.to_string()
                })),
            )
        }
    }
}

async fn handle_update_rule(
    State(_engine): State<SharedEngine>,
    axum::extract::Path(_rule_id): axum::extract::Path<String>,
    Json(_req): Json<serde_json::Value>,
) -> impl IntoResponse {
    // For now, update requires removing and re-adding (simplified)
    // In production, you'd want more sophisticated update logic
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "message": "Rule update not yet implemented. Use DELETE + POST instead."
        })),
    )
}

async fn handle_delete_rule(
    State(engine): State<SharedEngine>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut engine_lock = engine.write().await;
    
    let rule_idx = engine_lock.rules.iter().position(|r| r.rule.id == rule_id);
    if let Some(idx) = rule_idx {
        engine_lock.rules.remove(idx);
        engine_lock.window_buffers.remove(&rule_id);
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Rule deleted successfully",
                "rule_id": rule_id
            })),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Rule not found",
                "rule_id": rule_id
            })),
        )
    }
}

// State Introspection API

async fn handle_state(
    State(engine): State<SharedEngine>,
) -> impl IntoResponse {
    let engine_lock = engine.read().await;
    let mut states = Vec::new();
    
    for rule in &engine_lock.rules {
        let last_result = engine_lock.state.get_last_result(&rule.rule.id).await
            .unwrap_or(crate::state::PredicateResult::False);
        
        let window_size = engine_lock.window_buffers
            .get(&rule.rule.id)
            .map(|b| {
                b.get_batches().iter().map(|batch| batch.num_rows()).sum::<usize>()
            })
            .unwrap_or(0);
        
        states.push(serde_json::json!({
            "rule_id": rule.rule.id,
            "rule_name": rule.rule.name,
            "current_state": match last_result {
                crate::state::PredicateResult::True => "active",
                crate::state::PredicateResult::False => "inactive",
            },
            "window_size": window_size,
            "enabled": rule.rule.enabled,
        }));
    }
    
    (StatusCode::OK, Json(serde_json::json!({ "states": states })))
}

async fn handle_rule_state(
    State(engine): State<SharedEngine>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let engine_lock = engine.read().await;
    
    // Find rule
    let rule = engine_lock.rules.iter().find(|r| r.rule.id == rule_id);
    if rule.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Rule not found",
                "rule_id": rule_id
            })),
        );
    }
    
    let rule = rule.unwrap();
    let last_result = engine_lock.state.get_last_result(&rule_id).await
        .unwrap_or(crate::state::PredicateResult::False);
    
    let window_size = engine_lock.window_buffers
        .get(&rule_id)
        .map(|b| {
            b.get_batches().iter().map(|batch| batch.num_rows()).sum::<usize>()
        })
        .unwrap_or(0);
    
    // Get activation count from metrics
    let metrics = crate::metrics::METRICS.snapshot();
    let activation_count = metrics.rule_activations.get(&rule_id).copied().unwrap_or(0);
    
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "rule_id": rule_id,
            "rule_name": rule.rule.name,
            "current_state": match last_result {
                crate::state::PredicateResult::True => "active",
                crate::state::PredicateResult::False => "inactive",
            },
            "activation_count": activation_count,
            "window_size": window_size,
            "enabled": rule.rule.enabled,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

async fn shutdown_signal(engine: SharedEngine, config_path: String) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Termination signal received (Ctrl+C)");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Termination signal received (SIGTERM)");
    };

    #[cfg(unix)]
    let reload = async {
        let mut stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .expect("failed to install SIGHUP handler");
        while stream.recv().await.is_some() {
            info!("SIGHUP received, reloading configuration...");
            match crate::config::FuseRuleConfig::from_file(&config_path) {
                Ok(new_config) => {
                    let mut engine_lock = engine.write().await;
                    if let Err(e) = engine_lock.reload_from_config(new_config).await {
                        error!("Failed to reload engine: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to load config file for reload: {}", e);
                }
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    #[cfg(not(unix))]
    let reload = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = reload => {},
    }
}

async fn handle_status() -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "active" })),
    )
}

async fn handle_health(State(engine): State<SharedEngine>) -> impl IntoResponse {
    let engine_lock = engine.read().await;
    let rule_count = engine_lock.rules.len();
    let enabled_rules = engine_lock.rules.iter().filter(|r| r.rule.enabled).count();
    let agent_count = engine_lock.agents.len();
    
    let health = serde_json::json!({
        "status": "healthy",
        "engine": {
            "rules_total": rule_count,
            "rules_enabled": enabled_rules,
            "rules_disabled": rule_count - enabled_rules,
            "agents": agent_count,
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    
    (StatusCode::OK, Json(health))
}

async fn handle_rules(State(engine): State<SharedEngine>) -> impl IntoResponse {
    let engine_lock = engine.read().await;
    let rules: Vec<Value> = engine_lock
        .rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.rule.id,
                "name": r.rule.name,
                "predicate": r.rule.predicate,
                "action": r.rule.action,
                "window_seconds": r.rule.window_seconds,
                "version": r.rule.version,
                "enabled": r.rule.enabled,
            })
        })
        .collect();
    
    (StatusCode::OK, Json(serde_json::json!({ "rules": rules })))
}

async fn handle_metrics() -> String {
    crate::metrics::METRICS.to_prometheus()
}

async fn handle_ingest_with_rate_limit(
    State(engine): State<SharedEngine>,
    Json(payload): Json<Value>,
    rate_limiter: Option<Arc<RateLimiter>>,
) -> (StatusCode, Json<Value>) {
    // Apply rate limiting if configured
    if let Some(limiter) = rate_limiter {
        if !limiter.allow().await {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "error": "Rate limit exceeded",
                    "message": "Too many requests. Please try again later."
                })),
            );
        }
    }
    
    handle_ingest(State(engine), Json(payload)).await
}

async fn handle_ingest(
    State(engine): State<SharedEngine>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let request_id = Uuid::new_v4().to_string();
    debug!(request_id = %request_id, "Received ingest request");
    
    // 1. Convert JSON to Arrow RecordBatch
    let json_data = match serde_json::to_vec(&payload) {
        Ok(data) => data,
        Err(e) => {
            error!(request_id = %request_id, error = %e, "Failed to serialize payload");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Invalid JSON payload: {}", e),
                    "request_id": request_id
                })),
            );
        }
    };
    let cursor = Cursor::new(json_data);

    let mut engine_lock = engine.write().await;
    let schema = engine_lock.schema();
    
    // 2. Validate schema before processing
    let reader = match ReaderBuilder::new(schema.clone()).build(cursor) {
        Ok(r) => r,
        Err(e) => {
            error!(request_id = %request_id, error = %e, "Schema validation failed");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Schema validation failed: {}. Expected schema: {:?}", e, schema),
                    "request_id": request_id
                })),
            );
        }
    };

    let mut all_traces = Vec::new();
    let iter = reader.into_iter();
    for batch_result in iter {
        match batch_result {
            Ok(batch) => {
                debug!(
                    request_id = %request_id,
                    rows = batch.num_rows(),
                    "Ingested batch"
                );
                
                // Schema evolution: validate and handle schema changes
                let batch_schema = batch.schema();
                if batch_schema != schema {
                    // Check if it's a compatible evolution (new fields added)
                    let mut compatible = true;
                    let expected_fields: std::collections::HashSet<_> = schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();
                    let actual_fields: std::collections::HashSet<_> = batch_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();
                    
                    // Check if all expected fields are present (allowing new fields)
                    for field_name in &expected_fields {
                        if !actual_fields.contains(field_name) {
                            compatible = false;
                            break;
                        }
                    }
                    
                    if compatible {
                        info!(
                            request_id = %request_id,
                            new_fields = ?actual_fields.difference(&expected_fields).collect::<Vec<_>>(),
                            "Schema evolution detected - new fields added"
                        );
                        // In a full implementation, we'd update the schema here
                        // For now, we just log and continue
                    } else {
                        warn!(
                            request_id = %request_id,
                            expected = ?schema,
                            actual = ?batch_schema,
                            "Schema mismatch - missing required fields"
                        );
                    }
                }
                
                match engine_lock.process_batch(&batch).await {
                    Ok(traces) => {
                        debug!(
                            request_id = %request_id,
                            trace_count = traces.len(),
                            "Engine processed batch"
                        );
                        // Add request_id to all traces
                        let traces_with_id: Vec<Value> = traces
                            .into_iter()
                            .map(|t| {
                                let mut trace_json = serde_json::to_value(&t).unwrap();
                                if let Some(obj) = trace_json.as_object_mut() {
                                    obj.insert("request_id".to_string(), serde_json::json!(request_id));
                                }
                                trace_json
                            })
                            .collect();
                        all_traces.extend(traces_with_id);
                    }
                    Err(e) => {
                        error!(
                            request_id = %request_id,
                            error = %e,
                            "Engine processing error"
                        );
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({
                                "error": format!("Engine error: {}", e),
                                "request_id": request_id
                            })),
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    request_id = %request_id,
                    error = %e,
                    "Reader error"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("JSON Reader error: {}", e),
                        "request_id": request_id
                    })),
                );
            }
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "message": "Processed",
            "request_id": request_id,
            "traces": all_traces
        })),
    )
}
