# Contributing to FuseRule

Thank you for your interest in contributing! FuseRule is built on principles of composability and observability.

## 🛠️ Development Environment

1.  **Install Rust**: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2.  **Clone the Repo**: `git clone https://github.com/hamzzy/fuserule`
3.  **Run Tests**: `cargo test`

## 🤝 How to Contribute

- **Add an "Edge"**: Implement the `Agent`, `StateStore`, or `RuleEvaluator` trait for a new backend (e.g., Redis, Kafka).
- **Bug Fixes**: Open an Issue before submitting a PR.
- **Documentation**: We value clear explanations of complex rules.

## 📜 Design Principles
When submitting code, ensure it aligns with our core principles:
1. Hard Core, Soft Edges.
2. Observable Semantics.
3. Design for Replay.
