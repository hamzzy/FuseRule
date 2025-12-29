use arrow_rule_agent::evaluator::DataFusionEvaluator;
use arrow_rule_agent::rule::Rule;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[test]
fn test_predicate_compilation() {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);

    let rule = Rule {
        id: "test".to_string(),
        name: "Test".to_string(),
        predicate: "price > 100 AND volume < 50".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
    };

    let compiled = evaluator.compile(rule, &Arc::new(schema));
    assert!(compiled.is_ok());
}

#[test]
fn test_aggregate_detection() {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
    ]);

    let rule_with_agg = Rule {
        id: "test_agg".to_string(),
        name: "Aggregate Test".to_string(),
        predicate: "AVG(price) > 100".to_string(),
        action: "logger".to_string(),
        window_seconds: Some(60),
        version: 1,
        enabled: true,
    };

    let compiled = evaluator.compile(rule_with_agg, &Arc::new(schema));
    assert!(compiled.is_ok());
    assert!(compiled.unwrap().has_aggregates);
}

#[test]
fn test_invalid_predicate() {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
    ]);

    let rule = Rule {
        id: "test".to_string(),
        name: "Test".to_string(),
        predicate: "nonexistent_field > 100".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
    };

    let compiled = evaluator.compile(rule, &Arc::new(schema));
    assert!(compiled.is_err());
}

