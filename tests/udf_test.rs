//! Tests for User-Defined Functions (UDFs) integration

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_rule::evaluator::{DataFusionEvaluator, RuleEvaluator};
use fuse_rule::rule::Rule;
use fuse_rule::udf::{BuiltinUdfs, UdfRegistry};
use std::sync::Arc;

#[tokio::test]
async fn test_udf_registry() {
    let mut registry = UdfRegistry::new();
    BuiltinUdfs::register_all(&mut registry);
    
    let functions = registry.list();
    assert!(functions.contains(&"abs".to_string()));
    assert!(functions.contains(&"upper".to_string()));
    assert!(functions.contains(&"lower".to_string()));
}

#[tokio::test]
async fn test_evaluator_with_udf_registry() {
    let registry = UdfRegistry::new();
    let evaluator = DataFusionEvaluator::with_udf_registry(registry).unwrap();
    
    // Verify UDF registry is accessible
    let functions = evaluator.udf_registry().list();
    assert!(!functions.is_empty());
}

#[tokio::test]
async fn test_rule_with_builtin_functions() {
    // Test that rules can use DataFusion's built-in functions
    // Note: Custom UDFs are not yet fully implemented, but the structure is in place
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("symbol", DataType::Utf8, true),
    ]);

    // Test with ABS function (DataFusion built-in)
    let rule = Rule {
        id: "test_abs".to_string(),
        name: "Test ABS".to_string(),
        predicate: "ABS(price) > 100".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
        description: Some("Test rule with ABS function".to_string()),
        tags: vec!["test".to_string()],
    };

    let compiled = evaluator.compile(rule, &Arc::new(schema.clone()));
    assert!(compiled.is_ok(), "Rule with ABS function should compile");

    // Test with UPPER function (DataFusion built-in)
    let rule2 = Rule {
        id: "test_upper".to_string(),
        name: "Test UPPER".to_string(),
        predicate: "UPPER(symbol) = 'AAPL'".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
        description: Some("Test rule with UPPER function".to_string()),
        tags: vec!["test".to_string()],
    };

    let compiled2 = evaluator.compile(rule2, &Arc::new(schema));
    assert!(compiled2.is_ok(), "Rule with UPPER function should compile");
}

#[tokio::test]
async fn test_rule_evaluation_with_functions() {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
    ]);

    let rule = Rule {
        id: "test".to_string(),
        name: "Test".to_string(),
        predicate: "ABS(price) > 50".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
        description: None,
        tags: Vec::new(),
    };

    let compiled = evaluator.compile(rule, &Arc::new(schema.clone())).unwrap();

    // Create batch with negative prices
    let price_array = Arc::new(Float64Array::from(vec![-100.0, -30.0, 60.0]));
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![price_array],
    ).unwrap();

    // Evaluate
    let results = evaluator.evaluate_batch(&batch, &[compiled], &[vec![]]).await.unwrap();
    
    // ABS(-100) = 100 > 50 = true
    // ABS(-30) = 30 > 50 = false
    // ABS(60) = 60 > 50 = true
    // So at least one row matches, result should be True
    assert!(matches!(results[0].0, fuse_rule::state::PredicateResult::True));
}

