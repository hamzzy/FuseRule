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
pub struct BuiltinUdfs;

impl BuiltinUdfs {
    /// Register all built-in UDFs to a registry
    pub fn register_all(registry: &mut UdfRegistry) {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{Signature, Volatility};
        
        // Register is_weekend UDF
        registry.register_udf(Arc::new(ScalarUDF::from(IsWeekendUdf::new())));
        
        // Register normalize_price UDF
        registry.register_udf(Arc::new(ScalarUDF::from(NormalizePriceUdf::new())));
        
        // Register extract_domain UDF
        registry.register_udf(Arc::new(ScalarUDF::from(ExtractDomainUdf::new())));
    }
}

// ============================================================================
// Built-in UDF Implementations
// ============================================================================

/// Check if a timestamp is on a weekend (Saturday or Sunday)
#[derive(Debug)]
struct IsWeekendUdf {
    signature: datafusion::logical_expr::Signature,
}

impl IsWeekendUdf {
    fn new() -> Self {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{Signature, Volatility};
        
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for IsWeekendUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "is_weekend"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[datafusion::arrow::datatypes::DataType]) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }

    fn invoke(&self, args: &[datafusion::logical_expr::ColumnarValue]) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
        use datafusion::logical_expr::ColumnarValue;
        
        let args = ColumnarValue::values_to_arrays(args)?;
        let timestamps = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
        
        let result: BooleanArray = timestamps
            .iter()
            .map(|ts| {
                ts.map(|t| {
                    // Convert Unix timestamp to day of week (0 = Monday, 6 = Sunday)
                    let days_since_epoch = t / 86400;
                    let day_of_week = ((days_since_epoch + 3) % 7) as i32; // Adjust for epoch being Thursday
                    day_of_week == 5 || day_of_week == 6 // Saturday or Sunday
                })
            })
            .collect();
        
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

/// Normalize price to N decimal places
#[derive(Debug)]
struct NormalizePriceUdf {
    signature: datafusion::logical_expr::Signature,
}

impl NormalizePriceUdf {
    fn new() -> Self {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{Signature, Volatility};
        
        Self {
            signature: Signature::exact(vec![DataType::Float64, DataType::Int32], Volatility::Immutable),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for NormalizePriceUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "normalize_price"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[datafusion::arrow::datatypes::DataType]) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Float64)
    }

    fn invoke(&self, args: &[datafusion::logical_expr::ColumnarValue]) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array};
        use datafusion::logical_expr::ColumnarValue;
        
        let args = ColumnarValue::values_to_arrays(args)?;
        let prices = args[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let decimals = args[1].as_any().downcast_ref::<Int32Array>().unwrap();
        
        let result: Float64Array = prices
            .iter()
            .zip(decimals.iter())
            .map(|(price, dec)| {
                match (price, dec) {
                    (Some(p), Some(d)) => {
                        let multiplier = 10_f64.powi(d);
                        Some((p * multiplier).round() / multiplier)
                    }
                    _ => None,
                }
            })
            .collect();
        
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

/// Extract domain from email or URL
#[derive(Debug)]
struct ExtractDomainUdf {
    signature: datafusion::logical_expr::Signature,
}

impl ExtractDomainUdf {
    fn new() -> Self {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{Signature, Volatility};
        
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for ExtractDomainUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "extract_domain"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[datafusion::arrow::datatypes::DataType]) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Utf8)
    }

    fn invoke(&self, args: &[datafusion::logical_expr::ColumnarValue]) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::logical_expr::ColumnarValue;
        
        let args = ColumnarValue::values_to_arrays(args)?;
        let inputs = args[0].as_any().downcast_ref::<StringArray>().unwrap();
        
        let result: StringArray = inputs
            .iter()
            .map(|input| {
                input.and_then(|s| {
                    // Check if it's an email
                    if let Some(at_pos) = s.find('@') {
                        return Some(s[at_pos + 1..].to_string());
                    }
                    
                    // Check if it's a URL
                    if let Some(domain_start) = s.find("://") {
                        let after_protocol = &s[domain_start + 3..];
                        if let Some(slash_pos) = after_protocol.find('/') {
                            return Some(after_protocol[..slash_pos].to_string());
                        }
                        return Some(after_protocol.to_string());
                    }
                    
                    // Return as-is if neither
                    Some(s.to_string())
                })
            })
            .collect();
        
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
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

    #[test]
    fn test_builtin_udfs_registration() {
        let mut registry = UdfRegistry::new();
        BuiltinUdfs::register_all(&mut registry);
        
        assert!(registry.list().contains(&"is_weekend".to_string()));
        assert!(registry.list().contains(&"normalize_price".to_string()));
        assert!(registry.list().contains(&"extract_domain".to_string()));
    }

    #[test]
    fn test_is_weekend_udf() {
        use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
        use datafusion::logical_expr::ColumnarValue;
        
        let udf = IsWeekendUdf::new();
        
        // Test timestamps: Friday, Saturday, Sunday, Monday
        // 2024-01-05 (Friday), 2024-01-06 (Saturday), 2024-01-07 (Sunday), 2024-01-08 (Monday)
        let timestamps = Arc::new(Int64Array::from(vec![
            1704412800, // Friday
            1704499200, // Saturday
            1704585600, // Sunday
            1704672000, // Monday
        ])) as ArrayRef;
        
        let result = udf.invoke(&[ColumnarValue::Array(timestamps)]).unwrap();
        let result_array = match result {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<BooleanArray>().unwrap().clone(),
            _ => panic!("Expected array"),
        };
        
        assert_eq!(result_array.value(0), false); // Friday
        assert_eq!(result_array.value(1), true);  // Saturday
        assert_eq!(result_array.value(2), true);  // Sunday
        assert_eq!(result_array.value(3), false); // Monday
    }

    #[test]
    fn test_normalize_price_udf() {
        use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array};
        use datafusion::logical_expr::ColumnarValue;
        
        let udf = NormalizePriceUdf::new();
        
        let prices = Arc::new(Float64Array::from(vec![99.999, 100.123, 50.555])) as ArrayRef;
        let decimals = Arc::new(Int32Array::from(vec![2, 2, 1])) as ArrayRef;
        
        let result = udf.invoke(&[
            ColumnarValue::Array(prices),
            ColumnarValue::Array(decimals),
        ]).unwrap();
        
        let result_array = match result {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<Float64Array>().unwrap().clone(),
            _ => panic!("Expected array"),
        };
        
        assert_eq!(result_array.value(0), 100.0);  // 99.999 -> 100.00
        assert_eq!(result_array.value(1), 100.12); // 100.123 -> 100.12
        assert_eq!(result_array.value(2), 50.6);   // 50.555 -> 50.6
    }

    #[test]
    fn test_extract_domain_udf() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::logical_expr::ColumnarValue;
        
        let udf = ExtractDomainUdf::new();
        
        let inputs = Arc::new(StringArray::from(vec![
            "user@example.com",
            "https://www.google.com/search",
            "http://localhost:8080/api",
            "plain-domain.com",
        ])) as ArrayRef;
        
        let result = udf.invoke(&[ColumnarValue::Array(inputs)]).unwrap();
        
        let result_array = match result {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>().unwrap().clone(),
            _ => panic!("Expected array"),
        };
        
        assert_eq!(result_array.value(0), "example.com");
        assert_eq!(result_array.value(1), "www.google.com");
        assert_eq!(result_array.value(2), "localhost:8080");
        assert_eq!(result_array.value(3), "plain-domain.com");
    }
}
