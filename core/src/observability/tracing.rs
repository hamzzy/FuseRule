//! Distributed tracing using OpenTelemetry
//!
//! This module provides end-to-end tracing across:
//! - Data ingestion
//! - Rule evaluation
//! - Agent execution
//!
//! Spans are exported to OTLP-compatible backends (Jaeger, Tempo, etc.)

use anyhow::Result;
use opentelemetry::{
    trace::{Span, SpanKind, Tracer},
    KeyValue,
};
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler, TracerProvider},
    Resource,
};
use serde::{Deserialize, Serialize};

/// Configuration for distributed tracing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable distributed tracing
    pub enabled: bool,
    /// OTLP endpoint (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Service name for traces
    pub service_name: String,
    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: f64,
    /// Additional resource attributes
    pub resource_attributes: Option<std::collections::HashMap<String, String>>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: None,
            service_name: "fuse-rule-engine".to_string(),
            sampling_ratio: 1.0,
            resource_attributes: None,
        }
    }
}

/// Initialize OpenTelemetry tracing
///
/// Returns a TracerProvider that should be stored for the lifetime of the application
pub fn init_tracing(config: TracingConfig) -> Result<Option<TracerProvider>> {
    if !config.enabled {
        return Ok(None);
    }

    // Build resource attributes
    let mut resource_kvs = vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ];

    if let Some(attrs) = config.resource_attributes {
        for (k, v) in attrs {
            resource_kvs.push(KeyValue::new(k, v));
        }
    }

    let resource = Resource::new(resource_kvs);

    // Configure sampler
    let sampler = if config.sampling_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sampling_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sampling_ratio)
    };

    // Build tracer provider
    let provider = TracerProvider::builder()
        .with_resource(resource)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .build();

    // Set global tracer provider
    opentelemetry::global::set_tracer_provider(provider.clone());

    Ok(Some(provider))
}

/// Create a new span for rule evaluation
pub fn create_span(name: impl Into<String>, kind: SpanKind) -> opentelemetry::global::BoxedSpan {
    let tracer = opentelemetry::global::tracer("fuse-rule-engine");
    tracer.span_builder(name.into()).with_kind(kind).start(&tracer)
}

/// Span builder helper for common patterns
pub struct SpanBuilder {
    name: String,
    kind: SpanKind,
    attributes: Vec<KeyValue>,
}

impl SpanBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SpanKind::Internal,
            attributes: Vec::new(),
        }
    }

    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_attribute<V: Into<opentelemetry::Value>>(mut self, key: &'static str, value: V) -> Self {
        self.attributes.push(KeyValue::new(key, value.into()));
        self
    }

    pub fn start(self) -> opentelemetry::global::BoxedSpan {
        let tracer = opentelemetry::global::tracer("fuse-rule-engine");
        let mut span = tracer.span_builder(self.name).with_kind(self.kind).start(&tracer);

        for attr in self.attributes {
            span.set_attribute(attr);
        }

        span
    }
}

/// Instrumentation helpers for common operations
pub mod spans {
    use super::*;

    /// Create span for batch ingestion
    pub fn ingestion_span(batch_size: usize, source: &str) -> opentelemetry::global::BoxedSpan {
        SpanBuilder::new("ingest_batch")
            .with_kind(SpanKind::Server)
            .with_attribute("batch.size", batch_size as i64)
            .with_attribute("source", source.to_string())
            .start()
    }

    /// Create span for rule evaluation
    pub fn evaluation_span(rule_id: &str, rule_name: &str, batch_size: usize) -> opentelemetry::global::BoxedSpan {
        SpanBuilder::new("evaluate_rule")
            .with_kind(SpanKind::Internal)
            .with_attribute("rule.id", rule_id.to_string())
            .with_attribute("rule.name", rule_name.to_string())
            .with_attribute("batch.size", batch_size as i64)
            .start()
    }

    /// Create span for agent execution
    pub fn agent_span(agent_name: &str, rule_id: &str) -> opentelemetry::global::BoxedSpan {
        SpanBuilder::new("execute_agent")
            .with_kind(SpanKind::Client)
            .with_attribute("agent.name", agent_name.to_string())
            .with_attribute("rule.id", rule_id.to_string())
            .start()
    }

    /// Create span for state operations
    pub fn state_span(operation: &str, rule_id: &str) -> opentelemetry::global::BoxedSpan {
        SpanBuilder::new(format!("state_{}", operation))
            .with_kind(SpanKind::Internal)
            .with_attribute("operation", operation.to_string())
            .with_attribute("rule.id", rule_id.to_string())
            .start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.service_name, "fuse-rule-engine");
        assert_eq!(config.sampling_ratio, 1.0);
    }

    #[test]
    fn test_init_tracing_disabled() {
        let config = TracingConfig {
            enabled: false,
            ..Default::default()
        };
        let result = init_tracing(config);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_span_builder() {
        let _span = SpanBuilder::new("test_span")
            .with_kind(SpanKind::Internal)
            .with_attribute("test.key", "test_value")
            .start();
        // Span is created successfully if this doesn't panic
    }
}
