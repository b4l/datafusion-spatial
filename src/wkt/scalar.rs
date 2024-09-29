use std::fmt::{Error, Write};

use geoarrow::geo_traits::*;

/// Create geometry to WKT representation.
pub fn geometry_to_wkt<W: Write>(
    geometry: &impl GeometryTrait,
    writer: &mut W,
) -> Result<(), Error> {
    use GeometryType::*;

    match geometry.as_type() {
        Point(point) => point_to_wkt(point, writer),
        LineString(linestring) => linestring_to_wkt(linestring, writer),
        Polygon(polygon) => polygon_to_wkt(polygon, writer),
        MultiPoint(multi_point) => multi_point_to_wkt(multi_point, writer),
        MultiLineString(mls) => multi_linestring_to_wkt(mls, writer),
        MultiPolygon(multi_polygon) => multi_polygon_to_wkt(multi_polygon, writer),
        GeometryCollection(gc) => geometry_collection_to_wkt(gc, writer),
        Rect(rect) => rect_to_wkt(rect, writer),
    }
}

pub fn point_to_wkt<W: Write, P: PointTrait>(point: &P, writer: &mut W) -> Result<(), Error> {
    writer.write_str("POINT")?;

    let x = point.x();
    let y = point.y();

    if point.dim() == 3 {
        writer.write_str(" Z")?;
    }

    writer.write_fmt(format_args!(" ({x:?} {y:?}"))?;

    // z .. n
    for nth in 2..point.dim() {
        writer.write_fmt(format_args!(" {:?}", point.nth_unchecked(nth)))?;
    }

    writer.write_char(')')?;

    Ok(())
}

pub fn linestring_to_wkt<W: Write>(
    linestring: &impl LineStringTrait,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("LINESTRING ")?;

    if linestring.dim() == 3 {
        writer.write_str("Z ")?;
    }

    if linestring.num_coords() != 0 {
        add_coords(writer, linestring.coords())?;
    } else {
        writer.write_str("EMPTY")?;
    }

    Ok(())
}

pub fn polygon_to_wkt<W: Write>(polygon: &impl PolygonTrait, writer: &mut W) -> Result<(), Error> {
    writer.write_str("POLYGON")?;

    if polygon.dim() == 3 {
        writer.write_str(" Z")?;
    }

    if let Some(exterior) = polygon.exterior() {
        if exterior.num_coords() != 0 {
            writer.write_str(" (")?;
            add_coords(writer, exterior.coords())?;
        } else {
            writer.write_str(" EMPTY")?;
            return Ok(());
        }
    } else {
        writer.write_str(" EMPTY")?;
        return Ok(());
    };

    for interior in polygon.interiors() {
        writer.write_char(',')?;
        add_coords(writer, interior.coords())?;
    }

    writer.write_char(')')?;

    Ok(())
}

pub fn multi_point_to_wkt<W: Write>(
    multi_point: &impl MultiPointTrait,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTIPOINT")?;

    if multi_point.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut points = multi_point.points();

    if let Some(first) = points.next() {
        writer.write_str(" (")?;

        add_point(writer, first)?;

        for point in points {
            writer.write_char(',')?;
            add_point(writer, point)?;
        }

        writer.write_char(')')?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn multi_linestring_to_wkt<W: Write>(
    multi_linestring: &impl MultiLineStringTrait,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTILINESTRING")?;

    if multi_linestring.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut lines = multi_linestring.lines();

    if let Some(linestring) = lines.next() {
        writer.write_str(" (")?;
        add_coords(writer, linestring.coords())?;

        for linestring in lines {
            writer.write_char(',')?;
            add_coords(writer, linestring.coords())?;
        }

        writer.write_char(')')?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn multi_polygon_to_wkt<W: Write>(
    multi_polygon: &impl MultiPolygonTrait,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTIPOLYGON")?;

    if multi_polygon.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut polygons = multi_polygon.polygons();

    if let Some(polygon) = polygons.next() {
        writer.write_str(" ((")?;

        add_coords(writer, polygon.exterior().unwrap().coords())?;
        for interior in polygon.interiors() {
            writer.write_char(',')?;
            add_coords(writer, interior.coords())?;
        }

        for polygon in polygons {
            writer.write_str("),(")?;

            add_coords(writer, polygon.exterior().unwrap().coords())?;
            for interior in polygon.interiors() {
                writer.write_char(',')?;
                add_coords(writer, interior.coords())?;
            }
        }

        writer.write_str("))")?;
    } else {
        writer.write_str(" EMPTY")?;
    };

    Ok(())
}

pub fn geometry_collection_to_wkt<W: Write>(
    gc: &impl GeometryCollectionTrait,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("GEOMETRYCOLLECTION")?;

    if gc.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut geometries = gc.geometries();

    if let Some(first) = geometries.next() {
        writer.write_str(" (")?;

        geometry_to_wkt(&first, writer)?;

        for geom in geometries {
            writer.write_char(',')?;
            geometry_to_wkt(&geom, writer)?;
        }

        writer.write_char(')')?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn rect_to_wkt<W: Write>(rect: &impl RectTrait, writer: &mut W) -> Result<(), Error> {
    writer.write_str("POLYGON")?;

    let lower = rect.lower();
    let upper = rect.upper();

    match rect.dim() {
        2 => writer.write_fmt(format_args!(
            " ({0:?} {1:?},{2:?} {1:?},{2:?} {3:?},{0:?} {3:?},{0:?} {1:?})",
            lower.x(),
            lower.y(),
            upper.x(),
            upper.y(),
        ))?,
        3 => todo!("cube as polygon / linestring / multipoint?"),

        _ => unimplemented!(),
    };

    Ok(())
}

fn add_coord<W: Write, C: CoordTrait>(writer: &mut W, coord: C) -> Result<(), Error> {
    // x y
    writer.write_fmt(format_args!("{:?} {:?}", coord.x(), coord.y()))?;

    // z .. n
    for nth in 2..coord.dim() {
        writer.write_fmt(format_args!(" {:?}", coord.nth_unchecked(nth)))?;
    }

    Ok(())
}

fn add_point<W: Write, P: PointTrait>(writer: &mut W, point: P) -> Result<(), Error> {
    // x y
    writer.write_fmt(format_args!("({:?} {:?}", point.x(), point.y()))?;

    // z .. n
    for nth in 2..point.dim() {
        writer.write_fmt(format_args!(" {:?}", point.nth_unchecked(nth)))?;
    }

    writer.write_char(')')?;

    Ok(())
}

fn add_coords<W: Write, C: CoordTrait>(
    writer: &mut W,
    mut coords: impl Iterator<Item = C>,
) -> Result<(), Error> {
    writer.write_char('(')?;

    let first = coords.next().unwrap();
    add_coord(writer, first)?;

    for coord in coords {
        writer.write_char(',')?;
        add_coord(writer, coord)?;
    }

    writer.write_char(')')?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::buffer::OffsetBuffer;
    use geoarrow::{
        array::{CoordBuffer, InterleavedCoordBuffer},
        scalar::{
            OwnedLineString, OwnedMultiLineString, OwnedMultiPoint, OwnedMultiPolygon, OwnedPoint,
            OwnedPolygon,
        },
    };

    use super::*;

    #[test]
    fn point() {
        let coords = InterleavedCoordBuffer::<2>::new(vec![1., 2.].into());
        let point = OwnedPoint::new(CoordBuffer::Interleaved(coords), 0);

        let mut wkt = String::new();
        point_to_wkt(&point, &mut wkt).unwrap();

        assert_eq!(&wkt, "POINT (1.0 2.0)");
    }

    #[test]
    fn linestring() {
        let coords = InterleavedCoordBuffer::<2>::new(vec![1., 2., 3., 4., 5., 6.].into());
        let linestring = OwnedLineString::new(
            CoordBuffer::Interleaved(coords),
            OffsetBuffer::<i32>::new(vec![0, 3].into()),
            0,
        );

        let mut wkt = String::new();
        linestring_to_wkt(&linestring, &mut wkt).unwrap();

        assert_eq!(&wkt, "LINESTRING (1.0 2.0,3.0 4.0,5.0 6.0)");
    }

    #[test]
    fn polygon() {
        let coords = InterleavedCoordBuffer::<2>::new(vec![0., 0., 4., 0., 2., 4., 0., 0.].into());
        let polygon = OwnedPolygon::new(
            CoordBuffer::Interleaved(coords),
            OffsetBuffer::<i32>::new(vec![0, 1].into()),
            OffsetBuffer::<i32>::new(vec![0, 4].into()),
            0,
        );

        let mut wkt = String::new();
        polygon_to_wkt(&polygon, &mut wkt).unwrap();

        assert_eq!(&wkt, "POLYGON ((0.0 0.0,4.0 0.0,2.0 4.0,0.0 0.0))");
    }

    #[test]
    fn multi_point() {
        let coords = InterleavedCoordBuffer::<2>::new(vec![0., 0., 4., 0., 2., 4.].into());
        let multi_point = OwnedMultiPoint::new(
            CoordBuffer::Interleaved(coords),
            OffsetBuffer::<i32>::new(vec![0, 3].into()),
            0,
        );

        let mut wkt = String::new();
        multi_point_to_wkt(&multi_point, &mut wkt).unwrap();

        assert_eq!(&wkt, "MULTIPOINT ((0.0 0.0),(4.0 0.0),(2.0 4.0))");
    }

    #[test]
    fn multi_linestring() {
        let coords =
            InterleavedCoordBuffer::<2>::new(vec![1., 2., 3., 4., 5., 6., 7., 8., 9., 0.].into());
        let multi_linestring = OwnedMultiLineString::new(
            CoordBuffer::Interleaved(coords),
            OffsetBuffer::<i32>::new(vec![0, 2].into()),
            OffsetBuffer::<i32>::new(vec![0, 3, 5].into()),
            0,
        );

        let mut wkt = String::new();
        multi_linestring_to_wkt(&multi_linestring, &mut wkt).unwrap();

        assert_eq!(
            &wkt,
            "MULTILINESTRING ((1.0 2.0,3.0 4.0,5.0 6.0),(7.0 8.0,9.0 0.0))"
        );
    }

    #[test]
    fn multi_polygon() {
        let coords = InterleavedCoordBuffer::<2>::new(
            vec![
                0., 0., 4., 0., 2., 4., 0., 0., 4., 4., 8., 4., 8., 8., 4., 8., 4., 4.,
            ]
            .into(),
        );
        let multi_polygon = OwnedMultiPolygon::new(
            CoordBuffer::Interleaved(coords),
            OffsetBuffer::<i32>::new(vec![0, 2].into()),
            OffsetBuffer::<i32>::new(vec![0, 1, 2].into()),
            OffsetBuffer::<i32>::new(vec![0, 4, 9].into()),
            0,
        );

        let mut wkt = String::new();
        multi_polygon_to_wkt(&multi_polygon, &mut wkt).unwrap();

        assert_eq!(&wkt, "MULTIPOLYGON (((0.0 0.0,4.0 0.0,2.0 4.0,0.0 0.0)),((4.0 4.0,8.0 4.0,8.0 8.0,4.0 8.0,4.0 4.0)))");
    }
}
