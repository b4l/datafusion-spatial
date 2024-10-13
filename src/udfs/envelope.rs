use std::any::Any;

use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array},
        buffer::OffsetBuffer,
        datatypes::DataType,
    },
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use geo::BoundingRect;
use geoarrow::{
    array::{
        AsNativeArray, CoordBuffer, CoordType, LineStringArray, MultiLineStringArray,
        MultiPointArray, MultiPolygonArray, NativeArrayDyn, PointArray, PolygonArray,
        PolygonBuilder, PolygonCapacity, SeparatedCoordBufferBuilder, WKBArray,
    },
    datatypes::{Dimension, NativeType},
    error::GeoArrowError,
    scalar::OwnedPolygon,
    trait_::ArrayAccessor,
    ArrayBase, NativeArray,
};

use super::helpers::{coord_type, geom_type, native_type};

/// `ST_Envelope` user defined function (UDF) implementation.
#[derive(Debug, Clone)]
pub struct Envelope {
    signature: Signature,
    aliases: Vec<String>,
}

impl Envelope {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
            aliases: vec!["st_envelope".to_string()],
        }
    }
}

impl ScalarUDFImpl for Envelope {
    /// We implement as_any so that we can downcast the ScalarUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "ST_Envelope"
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
            DataType::Binary | DataType::LargeBinary => {
                Ok(NativeType::Polygon(CoordType::Separated, Dimension::XY).to_data_type())
            }
            dt => match coord_type(dt) {
                Some(_coord_type) => {
                    Ok(NativeType::Polygon(CoordType::Separated, Dimension::XY).to_data_type())
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type: `{dt}`"
                ))),
            },
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

        let mut builder: PolygonBuilder<2> =
            PolygonBuilder::new_with_options(CoordType::Separated, Default::default());

        match &geoms.data_type() {
            DataType::Binary => {
                let wkb: WKBArray<i32> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                for geom in wkb.iter_geo() {
                    builder
                        .push_polygon(
                            geom.and_then(|g| g.bounding_rect())
                                .map(|rect| rect.to_polygon())
                                .as_ref(),
                        )
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }
            }

            DataType::LargeBinary => {
                let wkb: WKBArray<i64> = WKBArray::try_from(geoms.as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                for geom in wkb.iter_geo() {
                    builder
                        .push_polygon(
                            geom.and_then(|g| g.bounding_rect())
                                .map(|rect| rect.to_polygon())
                                .as_ref(),
                        )
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }
            }

            _ => {
                let native_type = native_type(&args[0], geomtype);

                let geoms = NativeArrayDyn::from_arrow_array(
                    &geoms,
                    &native_type.to_field("geometry", true),
                )
                .unwrap();

                let envelopes = geoms.as_ref().envelope();

                return Ok(ColumnarValue::from(envelopes.to_array_ref() as ArrayRef));
            }
        }

        Ok(ColumnarValue::from(
            builder.finish().to_array_ref() as ArrayRef
        ))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

trait EnvelopeTrait {
    type Output;

    fn envelope(&self) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! array_envelope_impl {
    ($type:ty, $func:ident) => {
        impl<const D: usize> EnvelopeTrait for $type {
            type Output = PolygonArray<2>;

            fn envelope(&self) -> Self::Output {
                let n = self.iter().count();
                let capacity = PolygonCapacity::new(n * 5, n, n);
                let mut envelopes = PolygonBuilder::with_capacity_and_options(
                    capacity,
                    CoordType::Separated,
                    Default::default(),
                );

                for index in 0..self.len() {
                    match $func(self, index) {
                        Some(coords) => envelopes.push_polygon(Some(&envelope(&coords))).unwrap(),
                        None => envelopes
                            .push_polygon(None as Option<&OwnedPolygon<2>>)
                            .unwrap(),
                    }
                }

                envelopes.finish().into()
            }
        }
    };
}

array_envelope_impl!(PointArray<D>, point_coord_buffer);
array_envelope_impl!(LineStringArray<D>, line_string_coord_buffer);
array_envelope_impl!(PolygonArray<D>, polygon_coord_buffer);
array_envelope_impl!(MultiPointArray<D>, multi_point_coord_buffer);
array_envelope_impl!(MultiLineStringArray<D>, multi_line_string_coord_buffer);
array_envelope_impl!(MultiPolygonArray<D>, multi_polygon_coord_buffer);
// array_envelope_impl!(MixedGeometryArray<D>);
// array_envelope_impl!(GeometryCollectionArray<D>);
// envelope_array_impl!(RectArray<D>);

impl EnvelopeTrait for &dyn NativeArray {
    type Output = PolygonArray<2>;

    fn envelope(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().envelope(),
            LineString(_, XY) => self.as_line_string::<2>().envelope(),
            Polygon(_, XY) => self.as_polygon::<2>().envelope(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().envelope(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().envelope(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().envelope(),
            Mixed(_, XY) => unimplemented!(),
            GeometryCollection(_, XY) => unimplemented!(),
            Rect(XY) => unimplemented!(),
            Point(_, XYZ) => self.as_point::<3>().envelope(),
            LineString(_, XYZ) => self.as_line_string::<3>().envelope(),
            Polygon(_, XYZ) => self.as_polygon::<3>().envelope(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().envelope(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().envelope(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().envelope(),
            Mixed(_, XYZ) => unimplemented!(),
            GeometryCollection(_, XYZ) => unimplemented!(),
            Rect(XYZ) => unimplemented!(),
        }
    }
}

fn point_coord_buffer<const D: usize>(
    array: &PointArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        // hack for empty point
        if (array.coords().get_x(index) as f64).is_nan()
            && (array.coords().get_y(index) as f64).is_nan()
        {
            Some(array.coords().slice(index, 0))
        } else {
            Some(array.coords().slice(index, 1))
        }
    } else {
        None
    }
}

fn line_string_coord_buffer<const D: usize>(
    array: &LineStringArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        let offsets = array.geom_offsets().slice(index, 2);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(1) } as usize;
        Some(array.coords().slice(start, end - start))
    } else {
        None
    }
}

fn polygon_coord_buffer<const D: usize>(
    array: &PolygonArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        let offsets = array.geom_offsets().slice(index, 2);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(1) } as usize;
        let offsets = array.ring_offsets().slice(start, end - start);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(offsets.len() - 1) } as usize;
        Some(array.coords().slice(start, end - start))
    } else {
        None
    }
}

fn multi_point_coord_buffer<const D: usize>(
    array: &MultiPointArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        let offsets = array.geom_offsets().slice(index, 2);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(1) } as usize;
        Some(array.coords().slice(start, end - start))
    } else {
        None
    }
}

fn multi_line_string_coord_buffer<const D: usize>(
    array: &MultiLineStringArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        let offsets = array.geom_offsets().slice(index, 2);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(1) } as usize;
        let offsets = array.ring_offsets().slice(start, end - start);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(offsets.len() - 1) } as usize;
        Some(array.coords().slice(start, end - start))
    } else {
        None
    }
}

pub fn multi_polygon_coord_buffer<const D: usize>(
    array: &MultiPolygonArray<D>,
    index: usize,
) -> Option<CoordBuffer<D>> {
    if array.is_valid(index) {
        let offsets = array.geom_offsets().slice(index, 2);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(1) } as usize;
        let offsets = array.polygon_offsets().slice(start, end - start);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(offsets.len() - 1) } as usize;
        let offsets = array.ring_offsets().slice(start, end - start);
        let start = *unsafe { offsets.get_unchecked(0) } as usize;
        let end = *unsafe { offsets.get_unchecked(offsets.len() - 1) } as usize;
        Some(array.coords().slice(start, end - start))
    } else {
        None
    }
}

fn envelope<const D: usize>(coords: &CoordBuffer<D>) -> OwnedPolygon<2> {
    if coords.is_empty() {
        return OwnedPolygon::<2>::new(
            CoordBuffer::Separated(SeparatedCoordBufferBuilder::new().into()),
            OffsetBuffer::from_lengths([1]),
            OffsetBuffer::from_lengths([0]),
            0,
        );
    }

    let ((xmin, ymin), (xmax, ymax)) = match coords {
        CoordBuffer::Interleaved(coords) => coords.coords().chunks(D).fold(
            (
                (f64::INFINITY, f64::INFINITY),
                (f64::NEG_INFINITY, f64::NEG_INFINITY),
            ),
            |((mut xmin, mut ymin), (mut xmax, mut ymax)), coord| {
                let x = coord[0];
                let y = coord[1];

                if x < xmin {
                    xmin = x;
                } else if x > xmax {
                    xmax = x;
                }

                if y < ymin {
                    ymin = y;
                } else if y > ymax {
                    ymax = y;
                }

                ((xmin, ymin), (xmax, ymax))
            },
        ),
        CoordBuffer::Separated(coords) => {
            let xcoords = coords.coords()[0].clone();
            let ycoords = coords.coords()[1].clone();

            use datafusion::arrow::compute::{max, min};

            let xmin = min(&Float64Array::try_new(xcoords.clone(), None).unwrap()).unwrap();
            let ymin = min(&Float64Array::try_new(ycoords.clone(), None).unwrap()).unwrap();
            let xmax = max(&Float64Array::try_new(xcoords.clone(), None).unwrap()).unwrap();
            let ymax = max(&Float64Array::try_new(ycoords.clone(), None).unwrap()).unwrap();

            ((xmin, ymin), (xmax, ymax))
        }
    };

    let envelope_coords = SeparatedCoordBufferBuilder::from_vecs([
        vec![xmin, xmax, xmax, xmin, xmin],
        vec![ymin, ymin, ymax, ymax, ymin],
    ]);

    OwnedPolygon::<2>::new(
        CoordBuffer::Separated(envelope_coords.into()),
        OffsetBuffer::from_lengths([1]),
        OffsetBuffer::from_lengths([5]),
        0,
    )
}
