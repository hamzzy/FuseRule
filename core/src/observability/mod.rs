//! Observability module for production monitoring and debugging
//!
//! This module provides three core capabilities:
//! 1. Distributed tracing via OpenTelemetry
//! 2. Immutable audit logs for compliance and debugging
//! 3. Queryable trace history for operational insights

pub mod tracing;
pub mod audit;
pub mod trace_store;

#[cfg(test)]
mod tests;

pub use tracing::{TracingConfig, init_tracing, create_span};
pub use audit::{AuditLog, AuditEvent, AuditEventType};
pub use trace_store::{TraceStore, TraceQuery, TraceStoreConfig};
