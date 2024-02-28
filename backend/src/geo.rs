use geo::{HaversineDestination, Point, Rect};

pub fn get_bounding_box(center: Point, min_radius_metres: f64) -> Rect {
    // pythagoras
    let r_2 = min_radius_metres.powi(2);
    let corner_distance = (r_2 * 2.0).sqrt();

    Rect::new(
        // top left
        center.haversine_destination(315., corner_distance),
        // bottom right
        center.haversine_destination(135., corner_distance),
    )
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_get_bounding_box() {
        let center = Point::new(174.0, -36.0);
        let min_radius_metres = 2000.0;

        let bounding_box = get_bounding_box(center, min_radius_metres);

        println!("{:?}", bounding_box);
    }
}
