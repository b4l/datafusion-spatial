use std::any::Any;

use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType},
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use geoarrow::{
    array::{NativeArrayDyn, SerializedArray, WKBArray},
    error::GeoArrowError,
    ArrayBase, NativeArray,
};

use crate::{
    helpers::{geom_type, native_type},
    wkt::array::ToWKT,
};

/// `ST_AsText` user defined function (UDF) implementation.
#[derive(Debug, Clone)]
pub struct AsText {
    signature: Signature,
    aliases: Vec<String>,
}

impl AsText {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
            aliases: vec!["st_astext".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsText {
    /// We implement as_any so that we can downcast the ScalarUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "ST_AsText"
    }

    /// Return the "signature" of this function -- namely what types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What is the type of value that will be returned by this function? In
    /// this case it will always be a constant value, but it could also be a
    /// function of the input types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        match &arg_types[0] {
            DataType::Binary => Ok(DataType::Utf8),              // WKB
            DataType::LargeBinary => Ok(DataType::LargeUtf8),    // WKB
            DataType::List(_) => Ok(DataType::Utf8),             // geometries \ point
            DataType::FixedSizeList(_, _) => Ok(DataType::Utf8), // coords (interleaved)
            DataType::Struct(_) => Ok(DataType::Utf8),           // coords (separated)
            // DataType::Union(union_fields, union_mode) => todo!(),
            dt => Err(DataFusionError::Internal(format!(
                "Unsupported data type: `{dt}`"
            ))),
        }
    }

    /// This is the function that actually calculates the results.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // DataFusion has arranged for the correct inputs to be passed to this
        // function, but we check again to make sure
        assert_eq!(args.len(), 3);

        let geoms = match &args[0] {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };
        let geomtype = geom_type(&args[1])?;

        match geoms.data_type() {
            DataType::Binary => {
                let geoms: WKBArray<i32> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let wkt = geoms
                    .as_ref()
                    .to_wkt::<i32>()
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;

                Ok(ColumnarValue::from(wkt.to_array_ref() as ArrayRef))
            }

            DataType::LargeBinary => {
                let geoms: WKBArray<i64> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let wkt = geoms
                    .as_ref()
                    .to_wkt::<i64>()
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;

                Ok(ColumnarValue::from(wkt.to_array_ref() as ArrayRef))
            }
            _ => {
                let native_type = native_type(&args[0], geomtype);

                let geoms = NativeArrayDyn::from_arrow_array(
                    &geoms,
                    &native_type.to_field("geometry", true),
                )
                .unwrap();

                let wkt = geoms
                    .as_ref()
                    .to_wkt::<i32>()
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;

                Ok(ColumnarValue::from(wkt.to_array_ref() as ArrayRef))
            }
        }
    }

    /// We will also add an alias of "st_totext"
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
