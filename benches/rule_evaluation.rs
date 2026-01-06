//! Benchmark suite for FuseRule rule evaluation performance

use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuse_rule::evaluator::{DataFusionEvaluator, RuleEvaluator};
use fuse_rule::rule::Rule;
use fuse_rule::RuleEngine;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_batch(size: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);

    let prices: Vec<f64> = (0..size).map(|i| (i as f64) * 1.5).collect();
    let volumes: Vec<i32> = (0..size).map(|i| i * 100).collect();

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int32Array::from(volumes)),
        ],
    )
    .unwrap()
}

fn benchmark_simple_predicate(c: &mut Criterion) {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);

    let rule = Rule {
        id: "bench".to_string(),
        name: "Benchmark Rule".to_string(),
        predicate: "price > 100 AND volume < 1000".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
        description: None,
        tags: Vec::new(),
    };

    let compiled = evaluator
        .compile(rule, &Arc::new(schema))
        .expect("Rule should compile");

    let batch = create_test_batch(1000);

    c.bench_function("simple_predicate_1k_rows", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let batch = batch.clone();
                let compiled = compiled.clone();
                async move {
                    evaluator
                        .evaluate_batch(&batch, &[compiled], &[vec![]])
                        .await
                        .unwrap()
                }
            });
    });
}

fn benchmark_aggregate_predicate(c: &mut Criterion) {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);

    let rule = Rule {
        id: "bench_agg".to_string(),
        name: "Benchmark Aggregate".to_string(),
        predicate: "AVG(price) > 500".to_string(),
        action: "logger".to_string(),
        window_seconds: Some(60),
        version: 1,
        enabled: true,
        description: None,
        tags: Vec::new(),
    };

    let compiled = evaluator
        .compile(rule, &Arc::new(schema))
        .expect("Rule should compile");

    let batch = create_test_batch(1000);

    c.bench_function("aggregate_predicate_1k_rows", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let batch = batch.clone();
                let compiled = compiled.clone();
                async move {
                    evaluator
                        .evaluate_batch(&batch, &[compiled], &[vec![]])
                        .await
                        .unwrap()
                }
            });
    });
}

fn benchmark_multiple_rules(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("state");

    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);

    let mut engine = RuleEngine::new(
        Box::new(DataFusionEvaluator::new()),
        Box::new(fuse_rule::state::SledStateStore::new(&state_path).unwrap()),
        Arc::new(schema),
        1000,
        10,
    );

    // Add multiple rules
    for i in 0..10 {
        let rule = Rule {
            id: format!("rule_{}", i),
            name: format!("Rule {}", i),
            predicate: format!("price > {}", i * 100),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
            description: None,
            tags: Vec::new(),
        };
        futures::executor::block_on(engine.add_rule(rule)).unwrap();
    }

    let batch = create_test_batch(1000);

    c.bench_function("multiple_rules_10_rules_1k_rows", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let batch = batch.clone();
                async move { engine.process_batch(black_box(&batch)).await.unwrap() }
            });
    });
}

fn benchmark_rule_compilation(c: &mut Criterion) {
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("volume", DataType::Int32, true),
    ]);
    let schema = Arc::new(schema);

    c.bench_function("compile_simple_rule", |b| {
        b.iter(|| {
            let rule = Rule {
                id: "test".to_string(),
                name: "Test".to_string(),
                predicate: black_box("price > 100 AND volume < 1000".to_string()),
                action: "logger".to_string(),
                window_seconds: None,
                version: 1,
                enabled: true,
                description: None,
                tags: Vec::new(),
            };
            evaluator.compile(rule, &schema).unwrap()
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(100);
    targets = benchmark_simple_predicate, benchmark_aggregate_predicate, benchmark_multiple_rules, benchmark_rule_compilation
}

criterion_main!(benches);

