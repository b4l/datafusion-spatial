use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, OffsetSizeTrait, StringArray},
        datatypes::DataType,
    },
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
    scalar::ScalarValue,
};
use geoarrow::{
    array::WKBArray, error::GeoArrowError, io::wkb::WKBType, scalar::WKB, trait_::ArrayAccessor,
};

use super::helpers::scalar_arg_as_str;

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
                vec![TypeSignature::Any(1), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
            aliases: vec!["st_geometrytype".to_string()],
        }
    }
}

impl ScalarUDFImpl for GeometryType {
    /// To downcast the ScalarUDFImpl trait object
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

    /// The type of value that will be returned by this function.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
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
        let geomtype = scalar_arg_as_str(&args[1])?;

        match geoms.data_type() {
            DataType::Binary => {
                let geoms: WKBArray<i32> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let array = geoms
                    .iter()
                    .map(wkb_geom_to_type)
                    .collect::<Result<StringArray, DataFusionError>>()?;
                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }

            DataType::LargeBinary => {
                let geoms: WKBArray<i64> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                let array = geoms
                    .iter()
                    .map(wkb_geom_to_type)
                    .collect::<Result<StringArray, DataFusionError>>()?;

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            _ => {
                let geometry_type = format!("ST_{}", geomtype.replace(' ', ""));
                if geoms.as_ref().null_count() > 0 {
                    Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                        geoms
                            .as_ref()
                            .logical_nulls()
                            .unwrap()
                            .iter()
                            .map(|is_valid| if is_valid { Some(&geometry_type) } else { None }),
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        geometry_type,
                    ))))
                }
            }
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
