//! User-Defined Functions (UDFs) for extending SQL predicates
//!
//! This module provides a registry and loader for custom functions that can be
//! used in rule predicates, extending the built-in SQL functions available in DataFusion.

use anyhow::Result;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for managing UDFs
#[derive(Clone)]
pub struct UdfRegistry {
    functions: HashMap<String, Arc<ScalarUDF>>,
}

impl UdfRegistry {
    /// Create a new empty UDF registry
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a Scalar UDF
    pub fn register_udf(&mut self, udf: Arc<ScalarUDF>) {
        self.functions.insert(udf.name().to_string(), udf);
    }

    /// Register a UDF name (for built-ins or compatibility)
    /// This keeps the function in the list but doesn't hold an implementation
    /// if one isn't provided. For fully custom UDFs, use register_udf.
    pub fn register_name(&mut self, _name: String) {
        // No-op for name-only registration in this implementation
        // merging concepts for simplicity
    }

    /// Get all registered UDF names
    pub fn list(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    /// Register all UDFs in this registry to a DataFusion SessionContext
    pub fn register_to_context(&self, ctx: &SessionContext) -> Result<()> {
        for udf in self.functions.values() {
            ctx.register_udf((**udf).clone());
        }
        Ok(())
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Built-in UDFs that come with FuseRule
/// For now these are just placeholders or standard DataFusion functions
pub struct BuiltinUdfs;

impl BuiltinUdfs {
    /// Register all built-in UDF names to a registry
    /// Note: Actual function implementations rely on DataFusion's built-in functions
    /// for now. Custom UDFs can be added later using ScalarUDFImpl.
    pub fn register_all(_registry: &mut UdfRegistry) {
        // DataFusion comes with extensive built-ins (abs, round, upper, etc.)
        // We don't need to re-implement them here, but we could add custom helper functions
        // specific to rule engine logic if needed in the future.
        
        // This method is kept for compatibility and future expansion.
        // Currently it does nothing as DataFusion built-ins are auto-loaded by SessionContext.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Result as DFResult;
    use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::array::{ArrayRef, Float64Array};

    #[derive(Debug)]
    struct TestAddOneUdf {
        signature: Signature,
    }

    impl TestAddOneUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for TestAddOneUdf {
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

    #[test]
    fn test_udf_registry() {
        let mut registry = UdfRegistry::new();
        let udf = ScalarUDF::from(TestAddOneUdf::new());
        registry.register_udf(Arc::new(udf));
        
        assert!(registry.list().contains(&"add_one".to_string()));
    }
}
