use arrow::array::{BooleanArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_rule::evaluator::{DataFusionEvaluator, RuleEvaluator};
use fuse_rule::rule::Rule;
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #[test]
    fn test_predicate_compilation_property(
        price in -1000.0f64..10000.0f64,
        volume in 0i32..10000i32,
    ) {
        let evaluator = DataFusionEvaluator::new();
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("volume", DataType::Int32, true),
        ]);

        // Test that any numeric comparison compiles
        let predicates = vec![
            format!("price > {}", price),
            format!("price < {}", price),
            format!("price >= {}", price),
            format!("price <= {}", price),
            format!("price = {}", price),
            format!("volume > {}", volume),
            format!("volume < {}", volume),
            format!("price > {} AND volume < {}", price, volume),
            format!("price > {} OR volume < {}", price, volume),
        ];

        for predicate in predicates {
            let rule = Rule {
                id: "test".to_string(),
                name: "Test".to_string(),
                predicate: predicate.clone(),
                action: "logger".to_string(),
                window_seconds: None,
                version: 1,
                enabled: true,
            };

            let result = evaluator.compile(rule, &Arc::new(schema.clone()));
            prop_assert!(result.is_ok(), "Predicate '{}' should compile", predicate);
        }
    }

    #[test]
    fn test_rule_evaluation_consistency(
        prices in prop::collection::vec(-1000.0f64..10000.0f64, 1..100),
        threshold in -500.0f64..5000.0f64,
    ) {
        let evaluator = DataFusionEvaluator::new();
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, true),
        ]);

        let rule = Rule {
            id: "test".to_string(),
            name: "Test".to_string(),
            predicate: format!("price > {}", threshold),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
        };

        let compiled = evaluator.compile(rule, &Arc::new(schema.clone())).unwrap();

        // Create batch
        let price_array = Arc::new(Float64Array::from(prices.clone()));
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![price_array],
        ).unwrap();

        // Evaluate
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(
            evaluator.evaluate_batch(&batch, &[compiled], &[vec![]])
        ).unwrap();

        // Property: Result should be consistent with manual calculation
        let expected_true = prices.iter().any(|&p| p > threshold);
        let result = results[0].0;
        let actual_true = matches!(result, fuse_rule::state::PredicateResult::True);

        prop_assert_eq!(
            expected_true, actual_true,
            "Evaluation should match manual calculation. Prices: {:?}, Threshold: {}",
            prices, threshold
        );
    }

    #[test]
    fn test_boolean_logic_properties(
        a in any::<bool>(),
        b in any::<bool>(),
    ) {
        let evaluator = DataFusionEvaluator::new();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);

        // Create batch with boolean values
        let a_array = Arc::new(BooleanArray::from(vec![a]));
        let b_array = Arc::new(BooleanArray::from(vec![b]));
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![a_array, b_array],
        ).unwrap();

        // Test AND
        let and_rule = Rule {
            id: "and".to_string(),
            name: "AND".to_string(),
            predicate: "a AND b".to_string(),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
        };

        let compiled_and = evaluator.compile(and_rule, &Arc::new(schema.clone())).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let and_results = rt.block_on(
            evaluator.evaluate_batch(&batch, &[compiled_and], &[vec![]])
        ).unwrap();

        let expected_and = a && b;
        let actual_and = matches!(and_results[0].0, fuse_rule::state::PredicateResult::True);
        prop_assert_eq!(expected_and, actual_and, "AND logic should be correct");

        // Test OR
        let or_rule = Rule {
            id: "or".to_string(),
            name: "OR".to_string(),
            predicate: "a OR b".to_string(),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
        };

        let compiled_or = evaluator.compile(or_rule, &Arc::new(schema.clone())).unwrap();
        let or_results = rt.block_on(
            evaluator.evaluate_batch(&batch, &[compiled_or], &[vec![]])
        ).unwrap();

        let expected_or = a || b;
        let actual_or = matches!(or_results[0].0, fuse_rule::state::PredicateResult::True);
        prop_assert_eq!(expected_or, actual_or, "OR logic should be correct");
    }

    #[test]
    fn test_aggregate_functions_property(
        prices in prop::collection::vec(0.0f64..1000.0f64, 1..50),
    ) {
        let evaluator = DataFusionEvaluator::new();
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, true),
        ]);

        if prices.is_empty() {
            return Ok(());
        }

        let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
        let threshold = avg_price * 0.9; // 90% of average

        let rule = Rule {
            id: "agg".to_string(),
            name: "Aggregate".to_string(),
            predicate: format!("AVG(price) > {}", threshold),
            action: "logger".to_string(),
            window_seconds: Some(60),
            version: 1,
            enabled: true,
        };

        let compiled = evaluator.compile(rule, &Arc::new(schema.clone())).unwrap();
        prop_assert!(compiled.has_aggregates, "Should detect aggregate function");

        // Create batch
        let price_array = Arc::new(Float64Array::from(prices.clone()));
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![price_array],
        ).unwrap();

        // Evaluate
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(
            evaluator.evaluate_batch(&batch, &[compiled], &[vec![]])
        ).unwrap();

        // Property: AVG(price) > threshold should be true if avg_price > threshold
        let expected = avg_price > threshold;
        let actual = matches!(results[0].0, fuse_rule::state::PredicateResult::True);

        // Note: Due to floating point precision, we allow small differences
        prop_assert_eq!(
            expected, actual,
            "Aggregate evaluation should be consistent. Avg: {}, Threshold: {}",
            avg_price, threshold
        );
    }
}
