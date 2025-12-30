use crate::evaluator::RuleEvaluator;
use crate::RuleEngine;
use arrow_json::ReaderBuilder;
use axum::{
    extract::{Json, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub type SharedEngine = Arc<RwLock<RuleEngine>>;

/// API Key Authentication
#[derive(Clone)]
pub struct ApiKeyAuth {
    keys: HashSet<String>,
}

impl ApiKeyAuth {
    pub fn new(config_keys: Vec<String>) -> Self {
        let mut keys = HashSet::new();

        // Add keys from config file
        for key in config_keys {
            keys.insert(key);
        }

        // Add keys from environment variables (takes precedence)
        // FUSERULE_API_KEY - single key
        if let Ok(env_key) = std::env::var("FUSERULE_API_KEY") {
            if !env_key.is_empty() {
                keys.insert(env_key);
            }
        }

        // FUSERULE_API_KEYS - comma-separated keys
        if let Ok(env_keys) = std::env::var("FUSERULE_API_KEYS") {
            for key in env_keys.split(',') {
                let trimmed = key.trim();
                if !trimmed.is_empty() {
                    keys.insert(trimmed.to_string());
                }
            }
        }

        Self { keys }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn validate(&self, api_key: &str) -> bool {
        self.keys.contains(api_key)
    }
}

/// Authentication middleware
pub async fn auth_middleware(
    State(auth): State<ApiKeyAuth>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    // If no API keys configured, allow all requests
    if auth.is_empty() {
        return Ok(next.run(request).await);
    }

    // Extract API key from header
    let api_key = headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Validate API key
    if !auth.validate(api_key) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

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
    api_auth: ApiKeyAuth,
}

impl FuseRuleServer {
    pub fn new(
        engine: SharedEngine,
        config_path: String,
        rate_limit: Option<u32>,
        api_keys: Vec<String>,
    ) -> Self {
        let rate_limiter = rate_limit.map(|rps| Arc::new(RateLimiter::new(rps)));
        let api_auth = ApiKeyAuth::new(api_keys);
        Self {
            engine,
            config_path,
            rate_limiter,
            api_auth,
        }
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let rate_limiter = self.rate_limiter.clone();
        let api_auth = self.api_auth.clone();

        // Public routes (no auth required)
        let public_routes = Router::new()
            .route("/status", get(handle_status))
            .route("/health", get(handle_health))
            .route("/metrics", get(handle_metrics));

        // Protected routes (require API key if configured)
        let protected_routes = Router::new()
            .route("/rules", get(handle_rules))
            .route("/api/v1/rules", post(handle_create_rule))
            .route("/api/v1/rules/validate", post(handle_validate_rule))
            .route(
                "/api/v1/rules/:rule_id",
                axum::routing::put(handle_update_rule),
            )
            .route(
                "/api/v1/rules/:rule_id",
                axum::routing::patch(handle_patch_rule),
            )
            .route(
                "/api/v1/rules/:rule_id",
                axum::routing::delete(handle_delete_rule),
            )
            .route("/api/v1/state", get(handle_state))
            .route("/api/v1/state/:rule_id", get(handle_rule_state))
            .route(
                "/ingest",
                post(move |state, body| {
                    handle_ingest_with_rate_limit(state, body, rate_limiter.clone())
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                api_auth.clone(),
                auth_middleware,
            ));

        let app = Router::new()
            .merge(public_routes)
            .merge(protected_routes)
            .with_state(self.engine.clone());

        let addr = format!("0.0.0.0:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("FuseRule Server running on http://{}", addr);

        info!("Starting server with graceful shutdown handler");
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

async fn handle_validate_rule(
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

    let engine_lock = engine.read().await;
    let schema = engine_lock.schema();
    let evaluator = crate::evaluator::DataFusionEvaluator::new();

    let mut errors = Vec::new();
    let mut compiled = false;

    // Validate predicate compilation
    match evaluator.compile(rule.clone(), &schema) {
        Ok(_) => {
            compiled = true;
        }
        Err(e) => {
            errors.push(format!("Predicate compilation failed: {}", e));
        }
    }

    // Validate agent exists (if action specified)
    if !req.action.is_empty() && !engine_lock.agents.contains_key(&req.action) {
        errors.push(format!("Agent '{}' not found", req.action));
    }

    let valid = errors.is_empty() && compiled;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "valid": valid,
            "compiled": compiled,
            "errors": errors
        })),
    )
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
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "message": "Rule created successfully",
                "rule": rule
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Failed to add rule",
                "message": e.to_string()
            })),
        ),
    }
}

async fn handle_update_rule(
    State(engine): State<SharedEngine>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
    Json(req): Json<CreateRuleRequest>,
) -> impl IntoResponse {
    use crate::rule::Rule;

    // Ensure the rule_id in path matches the id in body
    if req.id != rule_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Rule ID in path does not match ID in body",
                "path_id": rule_id,
                "body_id": req.id
            })),
        );
    }

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
            // Validation successful
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

    // Update rule in engine
    let mut engine_lock = engine.write().await;
    match engine_lock.update_rule(&rule_id, rule.clone()).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Rule updated successfully",
                "rule": rule
            })),
        ),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Failed to update rule",
                "message": e.to_string()
            })),
        ),
    }
}

#[derive(serde::Deserialize)]
struct PatchRuleRequest {
    enabled: Option<bool>,
    action: Option<String>,
    name: Option<String>,
    predicate: Option<String>,
    window_seconds: Option<u64>,
}

async fn handle_patch_rule(
    State(engine): State<SharedEngine>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
    Json(req): Json<PatchRuleRequest>,
) -> impl IntoResponse {
    let mut engine_lock = engine.write().await;

    // Find rule
    let rule_idx = engine_lock.rules.iter().position(|r| r.rule.id == rule_id);
    if rule_idx.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Rule not found",
                "rule_id": rule_id
            })),
        );
    }

    let rule_idx = rule_idx.unwrap();
    let mut updated_rule = engine_lock.rules[rule_idx].rule.clone();

    // Apply partial updates
    if let Some(enabled) = req.enabled {
        updated_rule.enabled = enabled;
    }
    if let Some(action) = req.action {
        // Validate agent exists
        if !engine_lock.agents.contains_key(&action) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Agent not found",
                    "action": action
                })),
            );
        }
        updated_rule.action = action;
    }
    if let Some(name) = req.name {
        updated_rule.name = name;
    }
    if let Some(predicate) = req.predicate {
        // Validate predicate compiles
        let schema = engine_lock.schema();
        let evaluator = crate::evaluator::DataFusionEvaluator::new();
        let test_rule = crate::rule::Rule {
            id: updated_rule.id.clone(),
            name: updated_rule.name.clone(),
            predicate: predicate.clone(),
            action: updated_rule.action.clone(),
            window_seconds: updated_rule.window_seconds,
            version: updated_rule.version,
            enabled: updated_rule.enabled,
        };
        if evaluator.compile(test_rule, &schema).is_err() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid predicate",
                    "predicate": predicate
                })),
            );
        }
        updated_rule.predicate = predicate;
    }
    if let Some(window_seconds) = req.window_seconds {
        updated_rule.window_seconds = Some(window_seconds);
    }

    // Update rule using update_rule method
    match engine_lock
        .update_rule(&rule_id, updated_rule.clone())
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Rule updated successfully",
                "rule": updated_rule
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Failed to update rule",
                "message": e.to_string()
            })),
        ),
    }
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

async fn handle_state(State(engine): State<SharedEngine>) -> impl IntoResponse {
    let engine_lock = engine.read().await;
    let mut states = Vec::new();

    for rule in &engine_lock.rules {
        let last_result = engine_lock
            .state
            .get_last_result(&rule.rule.id)
            .await
            .unwrap_or(crate::state::PredicateResult::False);

        let window_size = engine_lock
            .window_buffers
            .get(&rule.rule.id)
            .map(|b| {
                b.get_batches()
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
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

    (
        StatusCode::OK,
        Json(serde_json::json!({ "states": states })),
    )
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
    let last_result = engine_lock
        .state
        .get_last_result(&rule_id)
        .await
        .unwrap_or(crate::state::PredicateResult::False);

    let last_transition = engine_lock
        .state
        .get_last_transition_time(&rule_id)
        .await
        .ok()
        .flatten();

    let window_size = engine_lock
        .window_buffers
        .get(&rule_id)
        .map(|b| {
            b.get_batches()
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>()
        })
        .unwrap_or(0);

    // Get activation count from metrics
    let metrics = crate::metrics::METRICS.snapshot();
    let activation_count = metrics.rule_activations.get(&rule_id).copied().unwrap_or(0);

    let mut response = serde_json::json!({
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
    });

    if let Some(transition_time) = last_transition {
        response.as_object_mut().unwrap().insert(
            "last_transition".to_string(),
            serde_json::Value::String(transition_time.to_rfc3339()),
        );
    }

    (StatusCode::OK, Json(response))
}

async fn shutdown_signal(engine: SharedEngine, config_path: String) {
    // Add a small delay to ensure logs can flush
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    info!("Setting up shutdown signal handlers");

    // Spawn reload handler as a background task (it runs forever)
    #[cfg(unix)]
    {
        let engine_clone = engine.clone();
        let config_path_clone = config_path.clone();
        tokio::spawn(async move {
            let mut stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("failed to install SIGHUP handler");
            info!("SIGHUP handler installed");
            // Process SIGHUP signals in a loop - this task runs forever
            loop {
                // Wait for SIGHUP signal
                if stream.recv().await.is_none() {
                    // Stream closed unexpectedly - this shouldn't happen, but if it does,
                    // we'll just wait forever to prevent shutdown
                    warn!("SIGHUP signal stream closed unexpectedly");
                    std::future::pending::<()>().await;
                }

                info!("SIGHUP received, reloading configuration...");
                match crate::config::FuseRuleConfig::from_file(&config_path_clone) {
                    Ok(new_config) => {
                        let mut engine_lock = engine_clone.write().await;
                        if let Err(e) = engine_lock.reload_from_config(new_config).await {
                            error!("Failed to reload engine: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to load config file for reload: {}", e);
                    }
                }
            }
        });
    }

    let ctrl_c = async {
        info!("Waiting for Ctrl+C signal...");
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Termination signal received (Ctrl+C)");
    };

    #[cfg(unix)]
    let terminate = async {
        info!("Waiting for SIGTERM signal...");
        let mut stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler");
        info!("SIGTERM handler installed, waiting for signal...");
        // Wait for SIGTERM - this future only completes when we actually receive the signal
        match stream.recv().await {
            Some(_) => {
                info!("Termination signal received (SIGTERM)");
            }
            None => {
                // Stream closed unexpectedly - wait forever to prevent shutdown
                warn!("SIGTERM signal stream closed unexpectedly - waiting indefinitely");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    info!("Entering shutdown signal select loop - server will run until Ctrl+C or SIGTERM");
    tokio::select! {
        _ = ctrl_c => {
            info!("Ctrl+C branch selected - shutting down");
        },
        _ = terminate => {
            info!("SIGTERM branch selected - shutting down");
        },
    }
    info!("Shutdown signal handler completed");
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
    // arrow-json ReaderBuilder expects NDJSON format (one JSON object per line)
    // or a single object, not a JSON array. Convert arrays to NDJSON format.
    let json_data = if payload.is_array() {
        // Convert array to NDJSON: each object on a newline
        let array = payload.as_array().unwrap();
        let mut ndjson = String::new();
        for (i, item) in array.iter().enumerate() {
            if i > 0 {
                ndjson.push('\n');
            }
            ndjson.push_str(&serde_json::to_string(item).unwrap_or_default());
        }
        ndjson.into_bytes()
    } else {
        // Single object - serialize as-is
        match serde_json::to_vec(&payload) {
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
                    let expected_fields: std::collections::HashSet<_> =
                        schema.fields().iter().map(|f| f.name()).collect();
                    let actual_fields: std::collections::HashSet<_> =
                        batch_schema.fields().iter().map(|f| f.name()).collect();

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
                                    obj.insert(
                                        "request_id".to_string(),
                                        serde_json::json!(request_id),
                                    );
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
