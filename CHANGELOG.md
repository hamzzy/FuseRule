# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-15

### Added
- Initial release of FuseRule
- Arrow-native data processing with zero-copy efficiency
- SQL-powered rule predicates using DataFusion
- Stateful rule transitions (Activated/Deactivated)
- Time window support for aggregate functions
- Multiple ingestion sources: HTTP, Kafka, WebSocket
- API key authentication
- Rate limiting on ingestion endpoints
- Handlebars templating for webhook actions
- State TTL support to prevent memory leaks
- Interactive REPL mode
- Rule debugger with step-through evaluation
- Property-based testing with proptest
- Comprehensive unit and integration tests
- Prometheus metrics export
- Hot-reload support via SIGHUP
- Circuit breakers for agent actions
- Backpressure handling with bounded channels
- Per-rule metrics and observability

### Features
- **Rule Engine**: Core evaluation engine with pluggable components
- **State Management**: Persistent state store with Sled backend
- **Agent System**: Pluggable agents (Logger, Webhook) with templates
- **Window Buffers**: Sliding time windows for temporal aggregations
- **CLI Tools**: Server, REPL, debugger, and validation commands
- **REST API**: Full CRUD API for rule management
- **State Query API**: Inspect rule states and transitions
- **Validation**: Rule predicate validation endpoint

### Documentation
- Comprehensive README with quickstart guide
- Architecture documentation
- API documentation with examples
- Code examples for common use cases

### Testing
- Unit tests for core components
- Integration tests for end-to-end scenarios
- Property-based tests for predicate evaluation

[0.1.0]: https://github.com/hamzzy/arrow-rule-agent/releases/tag/v0.1.0

