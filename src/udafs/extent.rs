use core::f64;
use std::{any::Any, str::FromStr};

use datafusion::{
    arrow::{
        array::{ArrayRef, AsArray},
        compute::min,
        datatypes::{DataType, Field, Fields, Float64Type},
    },
    common::scalar::ScalarStructBuilder,
    error::{DataFusionError, Result},
    logical_expr::{
        function::AccumulatorArgs, Accumulator, AggregateUDFImpl, ColumnarValue, Signature,
        TypeSignature, Volatility,
    },
    scalar::ScalarValue,
};
use geoarrow::{
    array::{AsNativeArray, NativeArrayDyn, WKBArray},
    datatypes::{Dimension, NativeType},
    error::GeoArrowError,
    io::parquet::metadata::GeoParquetGeometryType,
    NativeArray,
};

use crate::{compute::min_max_2d, helpers::native_type};

#[derive(Debug)]
pub struct Extent {
    signature: Signature,
    aliases: Vec<String>,
}

impl Extent {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
            aliases: vec!["st_extent".to_string()],
        }
    }
}

impl AggregateUDFImpl for Extent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Extent"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("xmin", DataType::Float64, false),
            Field::new("ymin", DataType::Float64, false),
            Field::new("xmax", DataType::Float64, false),
            Field::new("ymax", DataType::Float64, false),
        ])))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExtentAccumulator::new()))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
struct ExtentAccumulator {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

impl ExtentAccumulator {
    fn new() -> Self {
        Self {
            xmin: f64::MAX,
            ymin: f64::MAX,
            xmax: f64::MIN,
            ymax: f64::MIN,
        }
    }
}

impl Accumulator for ExtentAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.xmin),
            ScalarValue::from(self.xmax),
            ScalarValue::from(self.ymin),
            ScalarValue::from(self.ymax),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarStructBuilder::new()
            .with_scalar(
                Field::new("xmin", DataType::Float64, false),
                ScalarValue::Float64(Some(self.xmin)),
            )
            .with_scalar(
                Field::new("ymin", DataType::Float64, false),
                ScalarValue::Float64(Some(self.ymin)),
            )
            .with_scalar(
                Field::new("xmax", DataType::Float64, false),
                ScalarValue::Float64(Some(self.xmax)),
            )
            .with_scalar(
                Field::new("ymax", DataType::Float64, false),
                ScalarValue::Float64(Some(self.ymax)),
            )
            .build()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 3);

        match &values[0].data_type() {
            DataType::Binary => {
                let _wkb: WKBArray<i32> = WKBArray::try_from(values[0].as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                todo!()
            }
            DataType::LargeBinary => {
                let _wkb: WKBArray<i64> = WKBArray::try_from(values[0].as_ref())
                    .map_err(|e: GeoArrowError| DataFusionError::Internal(e.to_string()))?;

                todo!()
            }
            _ => {
                let geomtype =
                    GeoParquetGeometryType::from_str(values[1].as_string::<i32>().value(0))
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;

                let native_type = native_type(&ColumnarValue::Array(values[0].clone()), geomtype);

                let geoms = NativeArrayDyn::from_arrow_array(
                    &values[0],
                    &native_type.to_field("geometry", true),
                )
                .unwrap();

                use Dimension::*;

                let ((xmin, ymin), (xmax, ymax)) = match geoms.data_type() {
                    NativeType::Point(_, XY) => {
                        min_max_2d(geoms.as_ref().as_point::<2>().coords(), true)
                    }
                    NativeType::Point(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_point::<3>().coords(), true)
                    }
                    NativeType::LineString(_, XY) => {
                        min_max_2d(geoms.as_ref().as_line_string::<2>().coords(), false)
                    }
                    NativeType::LineString(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_line_string::<3>().coords(), false)
                    }
                    NativeType::Polygon(_, XY) => {
                        min_max_2d(geoms.as_ref().as_polygon::<2>().coords(), false)
                    }
                    NativeType::Polygon(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_polygon::<3>().coords(), false)
                    }
                    NativeType::MultiPoint(_, XY) => {
                        min_max_2d(geoms.as_ref().as_multi_point::<2>().coords(), false)
                    }
                    NativeType::MultiPoint(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_multi_point::<3>().coords(), false)
                    }
                    NativeType::MultiLineString(_, XY) => {
                        min_max_2d(geoms.as_ref().as_multi_line_string::<2>().coords(), false)
                    }
                    NativeType::MultiLineString(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_multi_line_string::<3>().coords(), false)
                    }
                    NativeType::MultiPolygon(_, XY) => {
                        min_max_2d(geoms.as_ref().as_multi_polygon::<2>().coords(), false)
                    }
                    NativeType::MultiPolygon(_, XYZ) => {
                        min_max_2d(geoms.as_ref().as_multi_polygon::<3>().coords(), false)
                    }
                    NativeType::Mixed(_, _) => unimplemented!(),
                    NativeType::GeometryCollection(_, _) => unimplemented!(),
                    NativeType::Rect(_) => unimplemented!(),
                };

                self.xmin = self.xmin.min(xmin);
                self.ymin = self.ymin.min(ymin);
                self.xmax = self.xmax.max(xmax);
                self.ymax = self.ymax.max(ymax);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.xmin = self
            .xmin
            .min(min(states[0].as_primitive::<Float64Type>()).unwrap());
        self.ymin = self
            .ymin
            .min(min(states[1].as_primitive::<Float64Type>()).unwrap());
        self.xmax = self
            .xmax
            .min(min(states[2].as_primitive::<Float64Type>()).unwrap());
        self.ymax = self
            .ymax
            .min(min(states[3].as_primitive::<Float64Type>()).unwrap());
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
