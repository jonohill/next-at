use sea_orm::{Linked, RelationDef, RelationTrait};

use crate::entity::{gtfs_agency, gtfs_routes};

use crate::entity::prelude::*;

pub struct TripAgency;

impl Linked for TripAgency {
    type FromEntity = GtfsTrips;
    type ToEntity = GtfsAgency;

    fn link(&self) -> Vec<RelationDef> {
        vec![
            gtfs_agency::Relation::GtfsRoutes.def(),
            gtfs_routes::Relation::GtfsAgency.def(),
        ]
    }
}
