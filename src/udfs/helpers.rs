use std::str::FromStr;

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::ColumnarValue,
    scalar::ScalarValue,
};
use geoarrow::io::parquet::metadata::{GeoParquetColumnEncoding, GeoParquetGeometryType};

pub fn scalar_arg_as_str(arg: &ColumnarValue) -> Result<&str> {
    match arg {
        ColumnarValue::Array(_encodings) => todo!(),
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) => match s {
                Some(s) => Ok(s.as_str()),
                None => todo!(),
            },
            _ => unimplemented!(),
        },
    }
}

pub fn geomtype(arg: &ColumnarValue) -> Result<GeoParquetGeometryType> {
    let s = scalar_arg_as_str(arg)?;

    GeoParquetGeometryType::from_str(s).map_err(|e| DataFusionError::Internal(e.to_string()))
}

pub fn encoding(arg: &ColumnarValue) -> Result<GeoParquetColumnEncoding> {
    match scalar_arg_as_str(arg)? {
        "WKB" => Ok(GeoParquetColumnEncoding::WKB),
        "point" => Ok(GeoParquetColumnEncoding::Point),
        "linestring" => Ok(GeoParquetColumnEncoding::LineString),
        "polygon" => Ok(GeoParquetColumnEncoding::Polygon),
        "multipoint" => Ok(GeoParquetColumnEncoding::MultiPoint),
        "multilinestring" => Ok(GeoParquetColumnEncoding::MultiLineString),
        "multipolygon" => Ok(GeoParquetColumnEncoding::MultiPolygon),
        enc => Err(DataFusionError::Internal(format!(
            "Unsupported geometry column encoding `{enc}`"
        ))),
    }
}
