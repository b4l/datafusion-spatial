use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    },
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use geoarrow::{array::WKBArray, error::GeoArrowError, trait_::GeometryArrayAccessor};
use geozero::ToWkt;

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
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
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
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    /// This is the function that actually calculates the results.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // DataFusion has arranged for the correct inputs to be passed to this
        // function, but we check again to make sure
        assert_eq!(args.len(), 1);

        match &args[0].data_type() {
            DataType::Binary => {
                let geoms: WKBArray<i32> = match &args[0] {
                    ColumnarValue::Scalar(geom) => WKBArray::try_from(geom.to_array()?.as_ref()),
                    ColumnarValue::Array(binary_array) => WKBArray::try_from(binary_array.as_ref()),
                }
                .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let array = geoms
                    .iter()
                    .map(|geom| match geom {
                        Some(wkb) => wkb
                            .to_wkt()
                            .map_err(|e| DataFusionError::Internal(e.to_string()))
                            .map(Some),
                        _ => Ok(None),
                    })
                    .collect::<Result<StringArray, DataFusionError>>()?;

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }

            DataType::LargeBinary => {
                let geoms: WKBArray<i64> = match &args[0] {
                    ColumnarValue::Scalar(geom) => WKBArray::try_from(geom.to_array()?.as_ref()),
                    ColumnarValue::Array(binary_array) => WKBArray::try_from(binary_array.as_ref()),
                }
                .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let array = geoms
                    .iter()
                    .map(|geom| match geom {
                        Some(wkb) => wkb
                            .to_wkt()
                            .map_err(|e| DataFusionError::Internal(e.to_string()))
                            .map(Some),
                        _ => Ok(None),
                    })
                    .collect::<Result<StringArray, DataFusionError>>()?;

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            _ => unreachable!(),
        }
    }

    /// We will also add an alias of "st_totext"
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
