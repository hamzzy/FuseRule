//! Rule Graph - Advanced rule semantics for decision workflows
//!
//! Transforms FuseRule from simple alerts into expressive decision workflows:
//!
//! - **Rule Chaining**: Rules trigger other rules in a DAG
//! - **Conditional Actions**: Multiple actions per rule, gated by runtime context
//! - **Dynamic Predicates**: Load predicates from DB/S3, A/B testing, feature flags
//! - **Cross-Rule Correlation**: Temporal patterns like "A AND B within 5 minutes"

pub mod chain;
pub mod conditional_action;
pub mod dynamic_predicate;
pub mod correlation;
pub mod dag;

pub use chain::{RuleChain, ChainedRule};
pub use conditional_action::{ConditionalAction, ActionCondition};
pub use dynamic_predicate::{DynamicPredicate, PredicateSource, PredicateLoader};
pub use correlation::{CorrelationPattern, TemporalWindow, CorrelationEngine};
pub use dag::{RuleDAG, DAGExecutor, ExecutionPlan};
