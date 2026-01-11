//! Tests for User-Defined Functions (UDFs) integration

use arrow::array::{Float64Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::common::Result as DFResult;
use fuse_rule_core::evaluator::{DataFusionEvaluator, RuleEvaluator};
use fuse_rule_core::rule::Rule;
use fuse_rule_core::udf::UdfRegistry;
use std::sync::Arc;

// Define a custom UDF: "add_one"
#[derive(Debug)]
struct AddOneUdf {
    signature: Signature,
}

impl AddOneUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AddOneUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "add_one"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let arr = args[0].as_any().downcast_ref::<Float64Array>().unwrap();
        
        let result: Float64Array = arr.iter().map(|v| v.map(|x| x + 1.0)).collect();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

#[tokio::test]
async fn test_custom_udf_registration() {
    // 1. Create Registry and Register UDF
    let mut registry = UdfRegistry::new();
    let udf = ScalarUDF::from(AddOneUdf::new());
    registry.register_udf(Arc::new(udf));

    // 2. Create Evaluator with Registry
    let evaluator = DataFusionEvaluator::with_udf_registry(registry).unwrap();
    
    // 3. Define Schema
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
    ]);

    // 4. Define Rule using Custom UDF
    // "add_one(price) > 100" means price + 1 > 100, so price > 99
    let rule = Rule {
        id: "test_udf".to_string(),
        name: "Test UDF".to_string(),
        predicate: "add_one(price) > 100".to_string(),
        action: "logger".to_string(),
        window_seconds: None,
        version: 1,
        enabled: true,
        description: Some("Test rule with custom UDF".to_string()),
        tags: vec!["test".to_string()],
    };

    // 5. Compile Rule
    let compiled = evaluator.compile(rule, &Arc::new(schema.clone()));
    assert!(compiled.is_ok(), "Should compile rule with custom UDF");
    let compiled_rule = compiled.unwrap();

    // 6. Evaluate Batch
    // 99.0 -> add_one -> 100.0 > 100 is False
    // 100.0 -> add_one -> 101.0 > 100 is True
    let price_array = Arc::new(Float64Array::from(vec![99.0, 100.0]));
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![price_array],
    ).unwrap();

    let results = evaluator.evaluate_batch(&batch, &[compiled_rule], &[vec![]]).await.unwrap();
    
    // Result logic:
    // Row 0: 99.0 -> 100.0 > 100 -> False
    // Row 1: 100.0 -> 101.0 > 100 -> True
    // evaluate_batch returns True if ANY row matches in the batch (for non-aggregates)
    // Here, Row 1 matches, so result should be True.
    assert!(matches!(results[0].0, fuse_rule_core::state::PredicateResult::True));
}
