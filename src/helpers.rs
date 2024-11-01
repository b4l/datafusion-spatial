use std::str::FromStr;

use datafusion::{
    arrow::datatypes::DataType,
    error::{DataFusionError, Result},
    logical_expr::ColumnarValue,
    scalar::ScalarValue,
};
use geoarrow::{
    array::CoordType,
    datatypes::{Dimension, NativeType},
    io::parquet::metadata::GeoParquetGeometryType,
};

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

pub fn geom_type(arg: &ColumnarValue) -> Result<GeoParquetGeometryType> {
    let s = scalar_arg_as_str(arg)?;

    GeoParquetGeometryType::from_str(s).map_err(|e| DataFusionError::Internal(e.to_string()))
}

// pub fn encoding(arg: &ColumnarValue) -> Result<GeoParquetColumnEncoding> {
//     match scalar_arg_as_str(arg)? {
//         "WKB" => Ok(GeoParquetColumnEncoding::WKB),
//         "point" => Ok(GeoParquetColumnEncoding::Point),
//         "linestring" => Ok(GeoParquetColumnEncoding::LineString),
//         "polygon" => Ok(GeoParquetColumnEncoding::Polygon),
//         "multipoint" => Ok(GeoParquetColumnEncoding::MultiPoint),
//         "multilinestring" => Ok(GeoParquetColumnEncoding::MultiLineString),
//         "multipolygon" => Ok(GeoParquetColumnEncoding::MultiPolygon),
//         enc => Err(DataFusionError::Internal(format!(
//             "Unsupported geometry column encoding `{enc}`"
//         ))),
//     }
// }

pub fn coord_type(data_type: &DataType) -> Option<CoordType> {
    match data_type {
        DataType::List(l1) => match l1.data_type() {
            DataType::FixedSizeList(_, _) => Some(CoordType::Interleaved),
            DataType::Struct(_) => Some(CoordType::Separated),
            DataType::List(l2) => match l2.data_type() {
                DataType::FixedSizeList(_, _) => Some(CoordType::Interleaved),
                DataType::Struct(_) => Some(CoordType::Separated),
                DataType::List(l1) => match l1.data_type() {
                    DataType::FixedSizeList(_, _) => Some(CoordType::Interleaved),
                    DataType::Struct(_) => Some(CoordType::Separated),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        },
        DataType::FixedSizeList(_, _) => Some(CoordType::Interleaved),
        DataType::Struct(_) => Some(CoordType::Separated),
        // DataType::Union(union_fields, union_mode) => todo!(),
        _ => None,
    }
}

// pub fn dimension(data_type: &DataType) -> Option<Dimension> {
//     let dimension_from_fields = |fields: &Fields| match fields.len() {
//         2 => Some(Dimension::XY),
//         3 => Some(Dimension::XYZ),
//         _ => None,
//     };

//     let dimension_from_size = |length: &i32| match length {
//         2 => Some(Dimension::XY),
//         3 => Some(Dimension::XYZ),
//         _ => None,
//     };

//     match data_type {
//         DataType::List(l1) => match l1.data_type() {
//             DataType::FixedSizeList(_, size) => dimension_from_size(size),
//             DataType::Struct(fields) => dimension_from_fields(fields),
//             DataType::List(l2) => match l2.data_type() {
//                 DataType::FixedSizeList(_, size) => dimension_from_size(size),
//                 DataType::Struct(fields) => dimension_from_fields(fields),
//                 DataType::List(l1) => match l1.data_type() {
//                     DataType::FixedSizeList(_, size) => dimension_from_size(size),
//                     DataType::Struct(fields) => dimension_from_fields(fields),
//                     _ => None,
//                 },
//                 _ => None,
//             },
//             _ => None,
//         },
//         DataType::FixedSizeList(_, size) => dimension_from_size(size),
//         DataType::Struct(fields) => dimension_from_fields(fields),
//         // DataType::Union(union_fields, union_mode) => todo!(),
//         _ => None,
//     }
// }

pub fn native_type(arg: &ColumnarValue, geometry_type: GeoParquetGeometryType) -> NativeType {
    let dt = arg.data_type();

    let ct = coord_type(&dt).unwrap_or(CoordType::Separated);

    use Dimension::*;
    use GeoParquetGeometryType::*;

    match geometry_type {
        Point => NativeType::Point(ct, XY),
        LineString => NativeType::LineString(ct, XY),
        Polygon => NativeType::Polygon(ct, XY),
        MultiPoint => NativeType::MultiPoint(ct, XY),
        MultiLineString => NativeType::MultiLineString(ct, XY),
        MultiPolygon => NativeType::MultiPolygon(ct, XY),
        GeometryCollection => NativeType::GeometryCollection(ct, XY),
        PointZ => NativeType::Point(ct, XYZ),
        LineStringZ => NativeType::LineString(ct, XYZ),
        PolygonZ => NativeType::Polygon(ct, XYZ),
        MultiPointZ => NativeType::MultiPoint(ct, XYZ),
        MultiLineStringZ => NativeType::MultiLineString(ct, XYZ),
        MultiPolygonZ => NativeType::MultiPolygon(ct, XYZ),
        GeometryCollectionZ => NativeType::GeometryCollection(ct, XYZ),
    }
}
