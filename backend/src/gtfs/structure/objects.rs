use chrono::{Datelike, NaiveDate, Weekday};
use rgb::RGB8;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use super::enums::ObjectType;
use super::enums::*;
use super::serde_helpers::*;

/// Objects that have an identifier implement this trait
///
/// Those identifier are technical and should not be shown to travellers
pub trait Id {
    /// Identifier of the object
    fn id(&self) -> &str;
}

impl<T: Id> Id for Arc<T> {
    fn id(&self) -> &str {
        self.as_ref().id()
    }
}

/// Trait to introspect what is the object’s type (stop, route…)
pub trait Type {
    /// What is the type of the object
    fn object_type(&self) -> ObjectType;
}

impl<T: Type> Type for Arc<T> {
    fn object_type(&self) -> ObjectType {
        self.as_ref().object_type()
    }
}

/// A calender describes on which days the vehicle runs. See <https://gtfs.org/reference/static/#calendartxt>
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtfsCalendar {
    /// Unique technical identifier (not for the traveller) of this calendar
    pub service_id: String,
    /// Does the service run on mondays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub monday: bool,
    /// Does the service run on tuesdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub tuesday: bool,
    /// Does the service run on wednesdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub wednesday: bool,
    /// Does the service run on thursdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub thursday: bool,
    /// Does the service run on fridays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub friday: bool,
    /// Does the service run on saturdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub saturday: bool,
    /// Does the service run on sundays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub sunday: bool,
    /// Start service day for the service interval
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
    pub start_date: NaiveDate,
    /// End service day for the service interval. This service day is included in the interval
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
    pub end_date: NaiveDate,
}

impl Type for GtfsCalendar {
    fn object_type(&self) -> ObjectType {
        ObjectType::Calendar
    }
}

impl Id for GtfsCalendar {
    fn id(&self) -> &str {
        &self.service_id
    }
}

impl fmt::Display for GtfsCalendar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}—{}", self.start_date, self.end_date)
    }
}

impl GtfsCalendar {
    /// Returns true if there is a service running on that day
    pub fn valid_weekday(&self, date: NaiveDate) -> bool {
        match date.weekday() {
            Weekday::Mon => self.monday,
            Weekday::Tue => self.tuesday,
            Weekday::Wed => self.wednesday,
            Weekday::Thu => self.thursday,
            Weekday::Fri => self.friday,
            Weekday::Sat => self.saturday,
            Weekday::Sun => self.sunday,
        }
    }
}

/// Defines a specific date that can be added or removed from a [Calendar]. See <https://gtfs.org/reference/static/#calendar_datestxt>
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtfsCalendarDate {
    /// Identifier of the service that is modified at this date
    pub service_id: String,
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
    /// Date where the service will be added or deleted
    pub date: NaiveDate,
    /// Is the service added or deleted
    pub exception_type: Exception,
}

/// A physical stop, station or area. See <https://gtfs.org/reference/static/#stopstxt>
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct GtfsStop {
    pub id: Option<i64>,
    /// Unique technical identifier (not for the traveller) of the stop
    pub stop_id: String,
    /// Short text or a number that identifies the location for riders
    pub stop_code: Option<String>,
    ///Name of the location. Use a name that people will understand in the local and tourist vernacular
    pub stop_name: String,
    /// Description of the location that provides useful, quality information
    pub stop_desc: String,
    /// Type of the location
    #[serde(default)]
    pub location_type: LocationType,
    /// Defines hierarchy between the different locations
    pub parent_station: Option<String>,
    /// Identifies the fare zone for a stop
    pub zone_id: Option<String>,
    /// URL of a web page about the location
    pub stop_url: Option<String>,
    /// Longitude of the stop
    #[serde(deserialize_with = "de_with_optional_float")]
    #[serde(serialize_with = "serialize_float_as_str")]
    pub stop_lon: Option<f64>,
    /// Latitude of the stop
    #[serde(deserialize_with = "de_with_optional_float")]
    #[serde(serialize_with = "serialize_float_as_str")]
    pub stop_lat: Option<f64>,
    /// Timezone of the location
    pub stop_timezone: Option<String>,
    /// Indicates whether wheelchair boardings are possible from the location
    #[serde(deserialize_with = "de_with_empty_default", default)]
    pub wheelchair_boarding: Availability,
    /// Level of the location. The same level can be used by multiple unlinked stations
    pub level_id: Option<String>,
    /// Platform identifier for a platform stop (a stop belonging to a station)
    pub platform_code: Option<String>,
}

impl Type for GtfsStop {
    fn object_type(&self) -> ObjectType {
        ObjectType::Stop
    }
}

impl Id for GtfsStop {
    fn id(&self) -> &str {
        &self.stop_id
    }
}

impl fmt::Display for GtfsStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.stop_name)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GtfsStopTime {
    /// [Trip] to which this stop time belongs to
    pub trip_id: String,
    /// Arrival time of the stop time.
    /// It's an option since the intermediate stops can have have no arrival
    /// and this arrival needs to be interpolated
    #[serde(
        deserialize_with = "deserialize_optional_time",
        serialize_with = "serialize_optional_time"
    )]
    pub arrival_time: Option<u32>,
    /// Departure time of the stop time.
    /// It's an option since the intermediate stops can have have no departure
    /// and this departure needs to be interpolated
    #[serde(
        deserialize_with = "deserialize_optional_time",
        serialize_with = "serialize_optional_time"
    )]
    pub departure_time: Option<u32>,
    /// Identifier of the [Stop] where the vehicle stops
    pub stop_id: String,
    /// Order of stops for a particular trip. The values must increase along the trip but do not need to be consecutive
    pub stop_sequence: u16,
    /// Text that appears on signage identifying the trip's destination to riders
    pub stop_headsign: Option<String>,
    /// Indicates pickup method
    #[serde(default)]
    pub pickup_type: PickupDropOffType,
    /// Indicates drop off method
    #[serde(default)]
    pub drop_off_type: PickupDropOffType,
    /// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
    #[serde(default)]
    pub continuous_pickup: ContinuousPickupDropOff,
    /// Indicates whether a rider can alight from the transit vehicle at any point along the vehicle’s travel path
    #[serde(default)]
    pub continuous_drop_off: ContinuousPickupDropOff,
    /// Actual distance traveled along the associated shape, from the first stop to the stop specified in this record. This field specifies how much of the shape to draw between any two stops during a trip
    pub shape_dist_traveled: Option<f32>,
    /// Indicates if arrival and departure times for a stop are strictly adhered to by the vehicle or if they are instead approximate and/or interpolated times
    #[serde(default)]
    pub timepoint: TimepointType,
}

/// A route is a commercial line (there can be various stop sequences for a same line). See <https://gtfs.org/reference/static/#routestxt>
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GtfsRoute {
    /// Unique technical (not for the traveller) identifier for the route
    pub route_id: String,
    /// Short name of a route. This will often be a short, abstract identifier like "32", "100X", or "Green" that riders use to identify a route, but which doesn't give any indication of what places the route serves
    #[serde(default)]
    pub route_short_name: String,
    /// Full name of a route. This name is generally more descriptive than the [Route::short_name]] and often includes the route's destination or stop
    #[serde(default)]
    pub route_long_name: String,
    /// Description of a route that provides useful, quality information
    pub route_desc: Option<String>,
    /// Indicates the type of transportation used on a route
    pub route_type: RouteType,
    /// URL of a web page about the particular route
    pub route_url: Option<String>,
    /// Agency for the specified route
    pub agency_id: Option<String>,
    /// Orders the routes in a way which is ideal for presentation to customers. Routes with smaller route_sort_order values should be displayed first.
    pub route_sort_order: Option<u32>,
    /// Route color designation that matches public facing material
    #[serde(
        deserialize_with = "deserialize_route_color",
        serialize_with = "serialize_color",
        default = "default_route_color"
    )]
    pub route_color: RGB8,
    /// Legible color to use for text drawn against a background of [Route::route_color]
    #[serde(
        deserialize_with = "deserialize_route_text_color",
        serialize_with = "serialize_color",
        default
    )]
    pub route_text_color: RGB8,
    /// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
    #[serde(default)]
    pub continuous_pickup: ContinuousPickupDropOff,
    /// Indicates whether a rider can alight from the transit vehicle at any point along the vehicle’s travel path
    #[serde(default)]
    pub continuous_drop_off: ContinuousPickupDropOff,
}

impl Type for GtfsRoute {
    fn object_type(&self) -> ObjectType {
        ObjectType::Route
    }
}

impl Id for GtfsRoute {
    fn id(&self) -> &str {
        &self.route_id
    }
}

impl fmt::Display for GtfsRoute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.route_long_name.is_empty() {
            write!(f, "{}", self.route_long_name)
        } else {
            write!(f, "{}", self.route_short_name)
        }
    }
}

/// A [Trip] where the relationships with other objects have not been checked
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GtfsTrip {
    /// Unique technical (not for the traveller) identifier for the Trip
    pub trip_id: String,
    /// References the [Calendar] on which this trip runs
    pub service_id: String,
    /// References along which [Route] this trip runs
    pub route_id: String,
    /// Shape of the trip
    pub shape_id: Option<String>,
    /// Text that appears on signage identifying the trip's destination to riders
    pub trip_headsign: Option<String>,
    /// Public facing text used to identify the trip to riders, for instance, to identify train numbers for commuter rail trips
    pub trip_short_name: Option<String>,
    /// Indicates the direction of travel for a trip. This field is not used in routing; it provides a way to separate trips by direction when publishing time tables
    pub direction_id: Option<DirectionType>,
    /// Identifies the block to which the trip belongs. A block consists of a single trip or many sequential trips made using the same vehicle, defined by shared service days and block_id. A block_id can have trips with different service days, making distinct blocks
    pub block_id: Option<String>,
    /// Indicates wheelchair accessibility
    #[serde(default)]
    pub wheelchair_accessible: Availability,
    /// Indicates whether bikes are allowed
    #[serde(default)]
    pub bikes_allowed: BikesAllowedType,
}

impl Type for GtfsTrip {
    fn object_type(&self) -> ObjectType {
        ObjectType::Trip
    }
}

impl Id for GtfsTrip {
    fn id(&self) -> &str {
        &self.trip_id
    }
}

impl fmt::Display for GtfsTrip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "route id: {}, service id: {}",
            self.route_id, self.service_id
        )
    }
}

/// General informations about the agency running the network. See <https://gtfs.org/reference/static/#agencytxt>
#[derive(Debug, Default, Clone)]
pub struct GtfsAgency {
    /// Unique technical (not for the traveller) identifier for the Agency
    pub agency_id: Option<String>,
    ///Full name of the transit agency
    pub agency_name: String,
    /// Full url of the transit agency.
    pub agency_url: String,
    /// Timezone where the transit agency is located
    pub agency_timezone: String,
    /// Primary language used by this transit agency
    pub agency_lang: Option<String>,
    /// A voice telephone number for the specified agency
    pub agency_phone: Option<String>,
    /// URL of a web page that allows a rider to purchase tickets or other fare instruments for that agency online
    pub agency_fare_url: Option<String>,
    /// Email address actively monitored by the agency’s customer service department
    pub agency_email: Option<String>,
}

impl Type for GtfsAgency {
    fn object_type(&self) -> ObjectType {
        ObjectType::Agency
    }
}

impl Id for GtfsAgency {
    fn id(&self) -> &str {
        match &self.agency_id {
            None => "",
            Some(id) => id,
        }
    }
}

impl fmt::Display for GtfsAgency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.agency_name)
    }
}

/// A single geographical point decribing the shape of a [Trip]. See <https://gtfs.org/reference/static/#shapestxt>
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GtfsShape {
    /// Unique technical (not for the traveller) identifier for the Shape
    pub shape_id: String,
    #[serde(default)]
    /// Latitude of a shape point
    pub shape_pt_lat: f64,
    /// Longitude of a shape point
    #[serde(default)]
    pub shape_pt_lon: f64,
    /// Sequence in which the shape points connect to form the shape. Values increase along the trip but do not need to be consecutive.
    pub shape_pt_sequence: usize,
    /// Actual distance traveled along the shape from the first shape point to the point specified in this record. Used by trip planners to show the correct portion of the shape on a map
    pub shape_dist_traveled: Option<f32>,
}

impl Type for GtfsShape {
    fn object_type(&self) -> ObjectType {
        ObjectType::Shape
    }
}

impl Id for GtfsShape {
    fn id(&self) -> &str {
        &self.shape_id
    }
}

/// Defines one possible fare. See <https://gtfs.org/reference/static/#fare_attributestxt>
#[derive(Debug, Serialize, Deserialize)]
pub struct FareAttribute {
    /// Unique technical (not for the traveller) identifier for the FareAttribute
    #[serde(rename = "fare_id")]
    pub id: String,
    /// Fare price, in the unit specified by [FareAttribute::currency]
    pub price: String,
    /// Currency used to pay the fare.
    #[serde(rename = "currency_type")]
    pub currency: String,
    ///Indicates when the fare must be paid
    pub payment_method: PaymentMethod,
    /// Indicates the number of transfers permitted on this fare
    pub transfers: Transfers,
    /// Identifies the relevant agency for a fare
    pub agency_id: Option<String>,
    /// Length of time in seconds before a transfer expires
    pub transfer_duration: Option<usize>,
}

impl Id for FareAttribute {
    fn id(&self) -> &str {
        &self.id
    }
}

impl Type for FareAttribute {
    fn object_type(&self) -> ObjectType {
        ObjectType::Fare
    }
}

/// A [Frequency] before being merged into the corresponding [Trip]
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawFrequency {
    /// References the [Trip] that uses frequency
    pub trip_id: String,
    /// Time at which the first vehicle departs from the first stop of the trip
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub start_time: u32,
    /// Time at which service changes to a different headway (or ceases) at the first stop in the trip
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub end_time: u32,
    /// Time, in seconds, between departures from the same stop (headway) for the trip, during the time interval specified by start_time and end_time
    pub headway_secs: u32,
    /// Indicates the type of service for a trip
    pub exact_times: Option<ExactTimes>,
}

/// Timetables can be defined by the frequency of their vehicles. See <<https://gtfs.org/reference/static/#frequenciestxt>>
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Frequency {
    /// Time at which the first vehicle departs from the first stop of the trip
    pub start_time: u32,
    /// Time at which service changes to a different headway (or ceases) at the first stop in the trip
    pub end_time: u32,
    /// Time, in seconds, between departures from the same stop (headway) for the trip, during the time interval specified by start_time and end_time
    pub headway_secs: u32,
    /// Indicates the type of service for a trip
    pub exact_times: Option<ExactTimes>,
}


/// Transfer information between stops before merged into [Stop]
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawTransfer {
    /// Stop from which to leave
    pub from_stop_id: String,
    /// Stop which to transfer to
    pub to_stop_id: String,
    /// Type of the transfer
    pub transfer_type: TransferType,
    /// Minimum time needed to make the transfer in seconds
    pub min_transfer_time: Option<u32>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
/// Transfer information between stops
pub struct StopTransfer {
    /// Stop which to transfer to
    pub to_stop_id: String,
    /// Type of the transfer
    pub transfer_type: TransferType,
    /// Minimum time needed to make the transfer in seconds
    pub min_transfer_time: Option<u32>,
}

impl From<RawTransfer> for StopTransfer {
    /// Converts from a [RawTransfer] to a [StopTransfer]
    fn from(transfer: RawTransfer) -> Self {
        Self {
            to_stop_id: transfer.to_stop_id,
            transfer_type: transfer.transfer_type,
            min_transfer_time: transfer.min_transfer_time,
        }
    }
}

/// Meta-data about the feed. See <https://gtfs.org/reference/static/#feed_infotxt>
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GtfsFeedInfo {
    /// Full name of the organization that publishes the dataset.
    pub feed_publisher_name: String,
    /// URL of the dataset publishing organization's website
    pub feed_publisher_url: String,
    /// Default language used for the text in this dataset
    pub feed_lang: String,
    /// Defines the language that should be used when the data consumer doesn’t know the language of the rider
    pub default_lang: Option<String>,
    /// The dataset provides complete and reliable schedule information for service in the period from this date
    #[serde(
        deserialize_with = "deserialize_option_date",
        serialize_with = "serialize_option_date",
        default
    )]
    pub feed_start_date: Option<NaiveDate>,
    ///The dataset provides complete and reliable schedule information for service in the period until this date
    #[serde(
        deserialize_with = "deserialize_option_date",
        serialize_with = "serialize_option_date",
        default
    )]
    pub feed_end_date: Option<NaiveDate>,
    /// String that indicates the current version of their GTFS dataset
    pub feed_version: Option<String>,
    /// Email address for communication regarding the GTFS dataset and data publishing practices
    pub feed_contact_email: Option<String>,
    /// URL for contact information, a web-form, support desk, or other tools for communication regarding the GTFS dataset and data publishing practices
    pub feed_contact_url: Option<String>,
}

impl fmt::Display for GtfsFeedInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.feed_publisher_name)
    }
}

/// A graph representation to describe subway or train, with nodes (the locations) and edges (the pathways).
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawPathway {
    /// Uniquely identifies the pathway
    #[serde(rename = "pathway_id")]
    pub id: String,
    /// Location at which the pathway begins
    pub from_stop_id: String,
    /// Location at which the pathway ends
    pub to_stop_id: String,
    /// Type of pathway between the specified (from_stop_id, to_stop_id) pair
    #[serde(rename = "pathway_mode")]
    pub mode: PathwayMode,
    /// Indicates in which direction the pathway can be used
    pub is_bidirectional: PathwayDirectionType,
    /// Horizontal length in meters of the pathway from the origin location to the destination location
    pub length: Option<f32>,
    /// Average time in seconds needed to walk through the pathway from the origin location to the destination location
    pub traversal_time: Option<u32>,
    /// Number of stairs of the pathway
    pub stair_count: Option<i32>,
    /// Maximum slope ratio of the pathway
    pub max_slope: Option<f32>,
    /// Minimum width of the pathway in meters
    pub min_width: Option<f32>,
    /// String of text from physical signage visible to transit riders
    pub signposted_as: Option<String>,
    /// Same than the signposted_as field, but when the pathways is used backward
    pub reversed_signposted_as: Option<String>,
}

impl Id for RawPathway {
    fn id(&self) -> &str {
        &self.id
    }
}

impl Type for RawPathway {
    fn object_type(&self) -> ObjectType {
        ObjectType::Pathway
    }
}

/// Pathway going from a stop to another.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pathway {
    /// Uniquely identifies the pathway
    pub id: String,
    /// Location at which the pathway ends
    pub to_stop_id: String,
    /// Type of pathway between the specified (from_stop_id, to_stop_id) pair
    pub mode: PathwayMode,
    /// Indicates in which direction the pathway can be used
    pub is_bidirectional: PathwayDirectionType,
    /// Horizontal length in meters of the pathway from the origin location to the destination location
    pub length: Option<f32>,
    /// Average time in seconds needed to walk through the pathway from the origin location to the destination location
    pub traversal_time: Option<u32>,
    /// Number of stairs of the pathway
    pub stair_count: Option<i32>,
    /// Maximum slope ratio of the pathway
    pub max_slope: Option<f32>,
    /// Minimum width of the pathway in meters
    pub min_width: Option<f32>,
    /// String of text from physical signage visible to transit riders
    pub signposted_as: Option<String>,
    /// Same than the signposted_as field, but when the pathways is used backward
    pub reversed_signposted_as: Option<String>,
}

impl Id for Pathway {
    fn id(&self) -> &str {
        &self.id
    }
}

impl Type for Pathway {
    fn object_type(&self) -> ObjectType {
        ObjectType::Pathway
    }
}

impl From<RawPathway> for Pathway {
    /// Converts from a [RawPathway] to a [Pathway]
    fn from(raw: RawPathway) -> Self {
        Self {
            id: raw.id,
            to_stop_id: raw.to_stop_id,
            mode: raw.mode,
            is_bidirectional: raw.is_bidirectional,
            length: raw.length,
            max_slope: raw.max_slope,
            min_width: raw.min_width,
            reversed_signposted_as: raw.reversed_signposted_as,
            signposted_as: raw.signposted_as,
            stair_count: raw.stair_count,
            traversal_time: raw.traversal_time,
        }
    }
}
