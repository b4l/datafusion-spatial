use core::f64;

use datafusion::arrow::{
    array::{AsArray, BooleanArray, Float64Array},
    compute::{filter, max, min},
    datatypes::Float64Type,
};

use geoarrow::array::CoordBuffer;

pub fn min_max_2d<const D: usize>(
    coords: &CoordBuffer<D>,
    empty_point_check: bool,
) -> ((f64, f64), (f64, f64)) {
    if coords.is_empty() {
        ((f64::MAX, f64::MAX), (f64::MIN, f64::MIN))
    } else {
        match coords {
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
                    }
                    if x > xmax && !x.is_nan() {
                        xmax = x;
                    }

                    if y < ymin {
                        ymin = y;
                    }
                    if y > ymax && !y.is_nan() {
                        ymax = y;
                    }

                    ((xmin, ymin), (xmax, ymax))
                },
            ),
            CoordBuffer::Separated(coords) => {
                let xcoords = coords.coords()[0].clone();
                let ycoords = coords.coords()[1].clone();

                let xcoords = Float64Array::try_new(xcoords, None).unwrap();
                let ycoords = Float64Array::try_new(ycoords, None).unwrap();

                // hack to work around empty points
                let (xcoords, ycoords) = if empty_point_check {
                    let xfilter = BooleanArray::from_unary(&xcoords, |x| !x.is_nan());
                    let xcoords = filter(&xcoords, &xfilter).unwrap();
                    let xcoords = xcoords.as_primitive::<Float64Type>().to_owned();

                    let yfilter = BooleanArray::from_unary(&ycoords, |y| !y.is_nan());
                    let ycoords = filter(&ycoords, &yfilter).unwrap();
                    let ycoords = ycoords.as_primitive::<Float64Type>().to_owned();

                    (xcoords, ycoords)
                } else {
                    (xcoords, ycoords)
                };

                let xmin = min(&xcoords).unwrap_or(f64::MAX);
                let ymin = min(&ycoords).unwrap_or(f64::MAX);
                let xmax = max(&xcoords).unwrap_or(f64::MIN);
                let ymax = max(&ycoords).unwrap_or(f64::MIN);

                ((xmin, ymin), (xmax, ymax))
            }
        }
    }
}
