use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, OffsetSizeTrait, StringArray},
        datatypes::DataType,
    },
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use geoarrow::{
    array::WKBArray, error::GeoArrowError, io::wkb::WKBType, scalar::WKB,
    trait_::NativeArrayAccessor,
};

/// `ST_GeometryType` user defined function (UDF) implementation.
#[derive(Debug, Clone)]
pub struct GeometryType {
    signature: Signature,
    aliases: Vec<String>,
}

impl GeometryType {
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
            aliases: vec!["st_geometrytype".to_string()],
        }
    }
}

impl ScalarUDFImpl for GeometryType {
    /// We implement as_any so that we can downcast the ScalarUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "ST_GeometryType"
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
                    .map(wkb_geom_to_type)
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
                    .map(wkb_geom_to_type)
                    .collect::<Result<StringArray, DataFusionError>>()?;

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn wkb_geom_to_type<O: OffsetSizeTrait>(
    geom: Option<WKB<O>>,
) -> Result<Option<String>, DataFusionError> {
    if let Some(wkb) = geom {
        wkb.wkb_type()
            .map_err(|e| DataFusionError::Internal(e.to_string()))
            .map(|wkb_type| {
                Some(match wkb_type {
                    WKBType::Point => "ST_Point".to_string(),
                    WKBType::LineString => "ST_LineString".to_string(),
                    WKBType::Polygon => "ST_Polygon".to_string(),
                    WKBType::MultiPoint => "ST_MultiPoint".to_string(),
                    WKBType::MultiLineString => "ST_MultiLineString".to_string(),
                    WKBType::MultiPolygon => "ST_MultiPolygon".to_string(),
                    WKBType::GeometryCollection => "ST_GeometryCollection".to_string(),
                    WKBType::PointZ => "ST_PointZ".to_string(),
                    WKBType::LineStringZ => "ST_LineStringZ".to_string(),
                    WKBType::PolygonZ => "ST_PolygonZ".to_string(),
                    WKBType::MultiPointZ => "ST_MultiPointZ".to_string(),
                    WKBType::MultiLineStringZ => "ST_MultiLineStringZ".to_string(),
                    WKBType::MultiPolygonZ => "ST_MultiPolygonZ".to_string(),
                    WKBType::GeometryCollectionZ => "ST_GeometryCollectionZ".to_string(),
                })
            })
    } else {
        Ok(None)
    }
}
