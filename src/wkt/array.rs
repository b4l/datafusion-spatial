use std::fmt::Error;

use datafusion::arrow::array::{builder::GenericStringBuilder, OffsetSizeTrait};

use geoarrow::{
    array::{
        AsNativeArray, AsSerializedArray, GeometryCollectionArray, LineStringArray,
        MixedGeometryArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
        PolygonArray, RectArray, SerializedArray, WKTArray,
    },
    datatypes::{Dimension, NativeType, SerializedType},
    trait_::ArrayAccessor,
    NativeArray,
};

use super::scalar::*;

pub trait ToWKT {
    fn to_wkt<O: OffsetSizeTrait>(&self) -> Result<WKTArray<O>, Error>;
}

// Implementation that iterates over geo objects
macro_rules! array_to_wkt_impl {
    ($type:ty, $func:ident) => {
        impl<const D: usize> ToWKT for $type {
            fn to_wkt<O: OffsetSizeTrait>(&self) -> Result<WKTArray<O>, Error> {
                let mut wkt_builder: GenericStringBuilder<O> = GenericStringBuilder::new();

                for item in self.iter() {
                    match item {
                        Some(geom) => {
                            $func(&geom, &mut wkt_builder)?;
                            wkt_builder.append_value("");
                        }
                        None => wkt_builder.append_null(),
                    }
                }

                Ok(wkt_builder.finish().into())
            }
        }
    };
}

array_to_wkt_impl!(PointArray<D>, point_to_wkt);
array_to_wkt_impl!(LineStringArray<D>, linestring_to_wkt);
array_to_wkt_impl!(PolygonArray<D>, polygon_to_wkt);
array_to_wkt_impl!(MultiPointArray<D>, multi_point_to_wkt);
array_to_wkt_impl!(MultiLineStringArray<D>, multi_linestring_to_wkt);
array_to_wkt_impl!(MultiPolygonArray<D>, multi_polygon_to_wkt);
array_to_wkt_impl!(MixedGeometryArray<D>, geometry_to_wkt);
array_to_wkt_impl!(GeometryCollectionArray<D>, geometry_collection_to_wkt);
array_to_wkt_impl!(RectArray<D>, rect_to_wkt);

impl ToWKT for &dyn NativeArray {
    fn to_wkt<O: OffsetSizeTrait>(&self) -> Result<WKTArray<O>, Error> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().to_wkt(),
            LineString(_, XY) => self.as_line_string::<2>().to_wkt(),
            Polygon(_, XY) => self.as_polygon::<2>().to_wkt(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().to_wkt(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().to_wkt(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().to_wkt(),
            Mixed(_, XY) => self.as_mixed::<2>().to_wkt(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().to_wkt(),
            Rect(XY) => self.as_rect::<2>().to_wkt(),
            Point(_, XYZ) => self.as_point::<3>().to_wkt(),
            LineString(_, XYZ) => self.as_line_string::<3>().to_wkt(),
            Polygon(_, XYZ) => self.as_polygon::<3>().to_wkt(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().to_wkt(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().to_wkt(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().to_wkt(),
            Mixed(_, XYZ) => self.as_mixed::<3>().to_wkt(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().to_wkt(),
            Rect(XYZ) => self.as_rect::<3>().to_wkt(),
        }
    }
}

impl ToWKT for &dyn SerializedArray {
    fn to_wkt<O: OffsetSizeTrait>(&self) -> Result<WKTArray<O>, Error> {
        let mut wkt_builder: GenericStringBuilder<O> = GenericStringBuilder::new();

        match self.data_type() {
            SerializedType::WKB => {
                for item in self.as_wkb().iter() {
                    match item {
                        Some(wkb) => {
                            geometry_to_wkt(&wkb.to_wkb_object(), &mut wkt_builder)?;
                            wkt_builder.append_value("");
                        }
                        None => wkt_builder.append_null(),
                    }
                }
            }
            SerializedType::LargeWKB => {
                for item in self.as_large_wkb().iter() {
                    match item {
                        Some(wkb) => {
                            geometry_to_wkt(&wkb.to_wkb_object(), &mut wkt_builder)?;
                            wkt_builder.append_value("");
                        }
                        None => wkt_builder.append_null(),
                    }
                }
            }
            SerializedType::WKT => todo!(),
            SerializedType::LargeWKT => todo!(),
        }

        Ok(wkt_builder.finish().into())
    }
}
