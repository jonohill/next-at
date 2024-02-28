// These were generated with prost from gtfs-realtime.proto

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use self::feed_header::Incrementality;
use super::serde_helpers::{deserialize_option_unix_date, Many, MaybeStringWrapped};

/// The contents of a feed message.
/// A feed is a continuous stream of feed messages. Each message in the stream is
/// obtained as a response to an appropriate HTTP GET request.
/// A realtime feed is always defined with relation to an existing GTFS feed.
/// All the entity ids are resolved with respect to the GTFS feed.
/// Note that "required" and "optional" as stated in this file refer to Protocol
/// Buffer cardinality, not semantic cardinality.  See reference.md at
/// <https://github.com/google/transit/tree/master/gtfs-realtime> for field
/// semantic cardinality.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct FeedMessage {
    /// Metadata about this feed and feed message.
    pub header: FeedHeader,
    /// Contents of the feed.
    pub entity: Vec<FeedEntity>,
}

/// Metadata about a feed, included in feed messages.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct FeedHeader {
    /// Version of the feed specification.
    /// The current version is 2.0.  Valid versions are "2.0", "1.0".
    pub gtfs_realtime_version: String,
    pub incrementality: Option<Incrementality>,
    /// This timestamp identifies the moment when the content of this feed has been
    /// created (in server time). In POSIX time (i.e., number of seconds since
    /// January 1st 1970 00:00:00 UTC).
    #[serde(deserialize_with = "deserialize_option_unix_date")]
    pub timestamp: Option<DateTime<Utc>>,
}
/// Nested message and enum types in `FeedHeader`.
pub mod feed_header {
    use serde_repr::Deserialize_repr;

    /// Determines whether the current fetch is incremental.  Currently,
    /// DIFFERENTIAL mode is unsupported and behavior is unspecified for feeds
    /// that use this mode.  There are discussions on the GTFS Realtime mailing
    /// list around fully specifying the behavior of DIFFERENTIAL mode and the
    /// documentation will be updated when those discussions are finalized.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
    #[repr(i32)]
    pub enum Incrementality {
        FullDataset = 0,
        Differential = 1,
    }
}
/// A definition (or update) of an entity in the transit feed.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct FeedEntity {
    /// The ids are used only to provide incrementality support. The id should be
    /// unique within a FeedMessage. Consequent FeedMessages may contain
    /// FeedEntities with the same id. In case of a DIFFERENTIAL update the new
    /// FeedEntity with some id will replace the old FeedEntity with the same id
    /// (or delete it - see is_deleted below).
    /// The actual GTFS entities (e.g. stations, routes, trips) referenced by the
    /// feed must be specified by explicit selectors (see EntitySelector below for
    /// more info).
    pub id: String,
    /// Whether this entity is to be deleted. Relevant only for incremental
    /// fetches.
    pub is_deleted: Option<bool>,
    /// Data about the entity itself. Exactly one of the following fields must be
    /// present (unless the entity is being deleted).
    pub trip_update: Option<TripUpdate>,

    pub vehicle: Option<VehiclePosition>,

    pub alert: Option<Alert>,
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub shape: Option<Shape>,
}
/// Realtime update of the progress of a vehicle along a trip.
/// Depending on the value of ScheduleRelationship, a TripUpdate can specify:
/// - A trip that proceeds along the schedule.
/// - A trip that proceeds along a route but has no fixed schedule.
/// - A trip that have been added or removed with regard to schedule.
///
/// The updates can be for future, predicted arrival/departure events, or for
/// past events that already occurred.
/// Normally, updates should get more precise and more certain (see
/// uncertainty below) as the events gets closer to current time.
/// Even if that is not possible, the information for past events should be
/// precise and certain. In particular, if an update points to time in the past
/// but its update's uncertainty is not 0, the client should conclude that the
/// update is a (wrong) prediction and that the trip has not completed yet.
///
/// Note that the update can describe a trip that is already completed.
/// To this end, it is enough to provide an update for the last stop of the trip.
/// If the time of that is in the past, the client will conclude from that that
/// the whole trip is in the past (it is possible, although inconsequential, to
/// also provide updates for preceding stops).
/// This option is most relevant for a trip that has completed ahead of schedule,
/// but according to the schedule, the trip is still proceeding at the current
/// time. Removing the updates for this trip could make the client assume
/// that the trip is still proceeding.
/// Note that the feed provider is allowed, but not required, to purge past
/// updates - this is one case where this would be practically useful.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct TripUpdate {
    /// The Trip that this message applies to. There can be at most one
    /// TripUpdate entity for each actual trip instance.
    /// If there is none, that means there is no prediction information available.
    /// It does *not* mean that the trip is progressing according to schedule.
    pub trip: TripDescriptor,
    /// Additional information on the vehicle that is serving this trip.
    pub vehicle: Option<VehicleDescriptor>,
    /// Updates to StopTimes for the trip (both future, i.e., predictions, and in
    /// some cases, past ones, i.e., those that already happened).
    /// The updates must be sorted by stop_sequence, and apply for all the
    /// following stops of the trip up to the next specified one.
    ///
    /// Example 1:
    /// For a trip with 20 stops, a StopTimeUpdate with arrival delay and departure
    /// delay of 0 for stop_sequence of the current stop means that the trip is
    /// exactly on time.
    ///
    /// Example 2:
    /// For the same trip instance, 3 StopTimeUpdates are provided:
    /// - delay of 5 min for stop_sequence 3
    /// - delay of 1 min for stop_sequence 8
    /// - delay of unspecified duration for stop_sequence 10
    /// This will be interpreted as:
    /// - stop_sequences 3,4,5,6,7 have delay of 5 min.
    /// - stop_sequences 8,9 have delay of 1 min.
    /// - stop_sequences 10,... have unknown delay.
    pub stop_time_update: Option<Many<trip_update::StopTimeUpdate>>,
    /// The most recent moment at which the vehicle's real-time progress was measured
    /// to estimate StopTimes in the future. When StopTimes in the past are provided,
    /// arrival/departure times may be earlier than this value. In POSIX
    /// time (i.e., the number of seconds since January 1st 1970 00:00:00 UTC).
    #[serde(deserialize_with = "deserialize_option_unix_date")]
    pub timestamp: Option<DateTime<Utc>>,
    /// The current schedule deviation for the trip.  Delay should only be
    /// specified when the prediction is given relative to some existing schedule
    /// in GTFS.
    ///
    /// Delay (in seconds) can be positive (meaning that the vehicle is late) or
    /// negative (meaning that the vehicle is ahead of schedule). Delay of 0
    /// means that the vehicle is exactly on time.
    ///
    /// Delay information in StopTimeUpdates take precedent of trip-level delay
    /// information, such that trip-level delay is only propagated until the next
    /// stop along the trip with a StopTimeUpdate delay value specified.
    ///
    /// Feed providers are strongly encouraged to provide a TripUpdate.timestamp
    /// value indicating when the delay value was last updated, in order to
    /// evaluate the freshness of the data.
    ///
    /// NOTE: This field is still experimental, and subject to change. It may be
    /// formally adopted in the future.
    pub delay: Option<i32>,

    pub trip_properties: Option<trip_update::TripProperties>,
}
/// Nested message and enum types in `TripUpdate`.
pub mod trip_update {
    use serde::Deserialize;

    /// Timing information for a single predicted event (either arrival or
    /// departure).
    /// Timing consists of delay and/or estimated time, and uncertainty.
    /// - delay should be used when the prediction is given relative to some
    ///    existing schedule in GTFS.
    /// - time should be given whether there is a predicted schedule or not. If
    ///    both time and delay are specified, time will take precedence
    ///    (although normally, time, if given for a scheduled trip, should be
    ///    equal to scheduled time in GTFS + delay).
    ///
    /// Uncertainty applies equally to both time and delay.
    /// The uncertainty roughly specifies the expected error in true delay (but
    /// note, we don't yet define its precise statistical meaning). It's possible
    /// for the uncertainty to be 0, for example for trains that are driven under
    /// computer timing control.
    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct StopTimeEvent {
        /// Delay (in seconds) can be positive (meaning that the vehicle is late) or
        /// negative (meaning that the vehicle is ahead of schedule). Delay of 0
        /// means that the vehicle is exactly on time.
        pub delay: Option<i32>,
        /// Event as absolute time.
        /// In Unix time (i.e., number of seconds since January 1st 1970 00:00:00
        /// UTC).
        pub time: Option<i64>,
        /// If uncertainty is omitted, it is interpreted as unknown.
        /// If the prediction is unknown or too uncertain, the delay (or time) field
        /// should be empty. In such case, the uncertainty field is ignored.
        /// To specify a completely certain prediction, set its uncertainty to 0.
        pub uncertainty: Option<i32>,
    }
    /// Realtime update for arrival and/or departure events for a given stop on a
    /// trip. Updates can be supplied for both past and future events.
    /// The producer is allowed, although not required, to drop past events.
    ///
    /// The update is linked to a specific stop either through stop_sequence or
    /// stop_id, so one of the fields below must necessarily be set.
    /// See the documentation in TripDescriptor for more information.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct StopTimeUpdate {
        /// Must be the same as in stop_times.txt in the corresponding GTFS feed.
        pub stop_sequence: Option<u32>,
        /// Must be the same as in stops.txt in the corresponding GTFS feed.
        pub stop_id: Option<String>,

        pub arrival: Option<StopTimeEvent>,

        pub departure: Option<StopTimeEvent>,

        /// Expected occupancy after departure from the given stop.
        /// Should be provided only for future stops.
        /// In order to provide departure_occupancy_status without either arrival or
        /// departure StopTimeEvents, ScheduleRelationship should be set to NO_DATA.
        pub departure_occupancy_status: Option<i32>,

        pub schedule_relationship: Option<stop_time_update::ScheduleRelationship>,
        /// Realtime updates for certain properties defined within GTFS stop_times.txt
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        pub stop_time_properties: Option<stop_time_update::StopTimeProperties>,
    }
    /// Nested message and enum types in `StopTimeUpdate`.
    pub mod stop_time_update {
        use serde::Deserialize;
        use serde_repr::Deserialize_repr;

        /// Provides the updated values for the stop time.
        /// NOTE: This message is still experimental, and subject to change. It may be formally adopted in the future.
        #[derive(Clone, PartialEq, Deserialize, Debug)]
        pub struct StopTimeProperties {
            /// Supports real-time stop assignments. Refers to a stop_id defined in the GTFS stops.txt.
            /// The new assigned_stop_id should not result in a significantly different trip experience for the end user than
            /// the stop_id defined in GTFS stop_times.txt. In other words, the end user should not view this new stop_id as an
            /// "unusual change" if the new stop was presented within an app without any additional context.
            /// For example, this field is intended to be used for platform assignments by using a stop_id that belongs to the
            /// same station as the stop originally defined in GTFS stop_times.txt.
            /// To assign a stop without providing any real-time arrival or departure predictions, populate this field and set
            /// StopTimeUpdate.schedule_relationship = NO_DATA.
            /// If this field is populated, it is preferred to omit `StopTimeUpdate.stop_id` and use only `StopTimeUpdate.stop_sequence`. If
            /// `StopTimeProperties.assigned_stop_id` and `StopTimeUpdate.stop_id` are populated, `StopTimeUpdate.stop_id` must match `assigned_stop_id`.
            /// Platform assignments should be reflected in other GTFS-realtime fields as well
            /// (e.g., `VehiclePosition.stop_id`).
            /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
            pub assigned_stop_id: Option<String>,
        }
        /// The relation between the StopTimeEvents and the static schedule.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
        #[repr(i32)]
        pub enum ScheduleRelationship {
            /// The vehicle is proceeding in accordance with its static schedule of
            /// stops, although not necessarily according to the times of the schedule.
            /// At least one of arrival and departure must be provided. If the schedule
            /// for this stop contains both arrival and departure times then so must
            /// this update. Frequency-based trips (GTFS frequencies.txt with exact_times = 0)
            /// should not have a SCHEDULED value and should use UNSCHEDULED instead.
            Scheduled = 0,
            /// The stop is skipped, i.e., the vehicle will not stop at this stop.
            /// Arrival and departure are optional.
            Skipped = 1,
            /// No StopTimeEvents are given for this stop.
            /// The main intention for this value is to give time predictions only for
            /// part of a trip, i.e., if the last update for a trip has a NO_DATA
            /// specifier, then StopTimeEvents for the rest of the stops in the trip
            /// are considered to be unspecified as well.
            /// Neither arrival nor departure should be supplied.
            NoData = 2,
            /// The vehicle is operating a trip defined in GTFS frequencies.txt with exact_times = 0.
            /// This value should not be used for trips that are not defined in GTFS frequencies.txt,
            /// or trips in GTFS frequencies.txt with exact_times = 1. Trips containing StopTimeUpdates
            /// with ScheduleRelationship=UNSCHEDULED must also set TripDescriptor.ScheduleRelationship=UNSCHEDULED.
            /// NOTE: This field is still experimental, and subject to change. It may be
            /// formally adopted in the future.
            Unscheduled = 3,
        }
    }
    /// Defines updated properties of the trip, such as a new shape_id when there is a detour. Or defines the
    /// trip_id, start_date, and start_time of a DUPLICATED trip.
    /// NOTE: This message is still experimental, and subject to change. It may be formally adopted in the future.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct TripProperties {
        /// Defines the identifier of a new trip that is a duplicate of an existing trip defined in (CSV) GTFS trips.txt
        /// but will start at a different service date and/or time (defined using the TripProperties.start_date and
        /// TripProperties.start_time fields). See definition of trips.trip_id in (CSV) GTFS. Its value must be different
        /// than the ones used in the (CSV) GTFS. Required if schedule_relationship=DUPLICATED, otherwise this field must not
        /// be populated and will be ignored by consumers.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        pub trip_id: Option<String>,
        /// Service date on which the DUPLICATED trip will be run, in YYYYMMDD format. Required if
        /// schedule_relationship=DUPLICATED, otherwise this field must not be populated and will be ignored by consumers.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        pub start_date: Option<String>,
        /// Defines the departure start time of the trip when it’s duplicated. See definition of stop_times.departure_time
        /// in (CSV) GTFS. Scheduled arrival and departure times for the duplicated trip are calculated based on the offset
        /// between the original trip departure_time and this field. For example, if a GTFS trip has stop A with a
        /// departure_time of 10:00:00 and stop B with departure_time of 10:01:00, and this field is populated with the value
        /// of 10:30:00, stop B on the duplicated trip will have a scheduled departure_time of 10:31:00. Real-time prediction
        /// delay values are applied to this calculated schedule time to determine the predicted time. For example, if a
        /// departure delay of 30 is provided for stop B, then the predicted departure time is 10:31:30. Real-time
        /// prediction time values do not have any offset applied to them and indicate the predicted time as provided.
        /// For example, if a departure time representing 10:31:30 is provided for stop B, then the predicted departure time
        /// is 10:31:30. This field is required if schedule_relationship is DUPLICATED, otherwise this field must not be
        /// populated and will be ignored by consumers.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        pub start_time: Option<String>,
        /// Specifies the shape of the vehicle travel path when the trip shape differs from the shape specified in
        /// (CSV) GTFS or to specify it in real-time when it's not provided by (CSV) GTFS, such as a vehicle that takes differing
        /// paths based on rider demand. See definition of trips.shape_id in (CSV) GTFS. If a shape is neither defined in (CSV) GTFS
        /// nor in real-time, the shape is considered unknown. This field can refer to a shape defined in the (CSV) GTFS in shapes.txt
        /// or a Shape in the (protobuf) real-time feed. The order of stops (stop sequences) for this trip must remain the same as
        /// (CSV) GTFS. Stops that are a part of the original trip but will no longer be made, such as when a detour occurs, should
        /// be marked as schedule_relationship=SKIPPED.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        pub shape_id: Option<String>,
    }
}
/// Realtime positioning information for a given vehicle.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct VehiclePosition {
    /// The Trip that this vehicle is serving.
    /// Can be empty or partial if the vehicle can not be identified with a given
    /// trip instance.
    pub trip: Option<TripDescriptor>,
    /// Additional information on the vehicle that is serving this trip.
    pub vehicle: Option<VehicleDescriptor>,
    /// Current position of this vehicle.
    pub position: Option<Position>,
    /// The stop sequence index of the current stop. The meaning of
    /// current_stop_sequence (i.e., the stop that it refers to) is determined by
    /// current_status.
    /// If current_status is missing IN_TRANSIT_TO is assumed.
    pub current_stop_sequence: Option<u32>,
    /// Identifies the current stop. The value must be the same as in stops.txt in
    /// the corresponding GTFS feed.
    pub stop_id: Option<String>,
    /// The exact status of the vehicle with respect to the current stop.
    /// Ignored if current_stop_sequence is missing.
    pub current_status: Option<vehicle_position::VehicleStopStatus>,
    /// Moment at which the vehicle's position was measured. In POSIX time
    /// (i.e., number of seconds since January 1st 1970 00:00:00 UTC).

    #[serde(deserialize_with = "deserialize_option_unix_date")]
    pub timestamp: Option<DateTime<Utc>>,

    pub congestion_level: Option<vehicle_position::CongestionLevel>,
    /// If multi_carriage_status is populated with per-carriage OccupancyStatus,
    /// then this field should describe the entire vehicle with all carriages accepting passengers considered.
    pub occupancy_status: Option<vehicle_position::OccupancyStatus>,
    /// A percentage value indicating the degree of passenger occupancy in the vehicle.
    /// The values are represented as an integer without decimals. 0 means 0% and 100 means 100%.
    /// The value 100 should represent the total maximum occupancy the vehicle was designed for,
    /// including both seated and standing capacity, and current operating regulations allow.
    /// The value may exceed 100 if there are more passengers than the maximum designed capacity.
    /// The precision of occupancy_percentage should be low enough that individual passengers cannot be tracked boarding or alighting the vehicle.
    /// If multi_carriage_status is populated with per-carriage occupancy_percentage,
    /// then this field should describe the entire vehicle with all carriages accepting passengers considered.
    /// This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub occupancy_percentage: Option<u32>,
    /// Details of the multiple carriages of this given vehicle.
    /// The first occurrence represents the first carriage of the vehicle,
    /// given the current direction of travel.
    /// The number of occurrences of the multi_carriage_details
    /// field represents the number of carriages of the vehicle.
    /// It also includes non boardable carriages,
    /// like engines, maintenance carriages, etc… as they provide valuable
    /// information to passengers about where to stand on a platform.
    /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
    pub multi_carriage_details: Option<Many<vehicle_position::CarriageDetails>>,
}
/// Nested message and enum types in `VehiclePosition`.
pub mod vehicle_position {
    use serde::Deserialize;
    use serde_repr::Deserialize_repr;

    /// Carriage specific details, used for vehicles composed of several carriages
    /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct CarriageDetails {
        /// Identification of the carriage. Should be unique per vehicle.
        pub id: Option<String>,
        /// User visible label that may be shown to the passenger to help identify
        /// the carriage. Example: "7712", "Car ABC-32", etc...
        /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
        pub label: Option<String>,
        /// Occupancy status for this given carriage, in this vehicle
        /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
        pub occupancy_status: Option<i32>,
        /// Occupancy percentage for this given carriage, in this vehicle.
        /// Follows the same rules as "VehiclePosition.occupancy_percentage"
        /// -1 in case data is not available for this given carriage (as protobuf defaults to 0 otherwise)
        /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
        pub occupancy_percentage: Option<i32>,
        /// Identifies the order of this carriage with respect to the other
        /// carriages in the vehicle's list of CarriageDetails.
        /// The first carriage in the direction of travel must have a value of 1.
        /// The second value corresponds to the second carriage in the direction
        /// of travel and must have a value of 2, and so forth.
        /// For example, the first carriage in the direction of travel has a value of 1.
        /// If the second carriage in the direction of travel has a value of 3,
        /// consumers will discard data for all carriages (i.e., the multi_carriage_details field).
        /// Carriages without data must be represented with a valid carriage_sequence number and the fields
        /// without data should be omitted (alternately, those fields could also be included and set to the "no data" values).
        /// This message/field is still experimental, and subject to change. It may be formally adopted in the future.
        pub carriage_sequence: Option<u32>,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
    #[repr(i32)]
    pub enum VehicleStopStatus {
        /// The vehicle is just about to arrive at the stop (on a stop
        /// display, the vehicle symbol typically flashes).
        IncomingAt = 0,
        /// The vehicle is standing at the stop.
        StoppedAt = 1,
        /// The vehicle has departed and is in transit to the next stop.
        InTransitTo = 2,
    }
    /// Congestion level that is affecting this vehicle.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
    #[repr(i32)]
    #[allow(clippy::enum_variant_names)]
    pub enum CongestionLevel {
        UnknownCongestionLevel = 0,
        RunningSmoothly = 1,
        StopAndGo = 2,
        Congestion = 3,
        /// People leaving their cars.
        SevereCongestion = 4,
    }
    /// The state of passenger occupancy for the vehicle or carriage.
    /// Individual producers may not publish all OccupancyStatus values. Therefore, consumers
    /// must not assume that the OccupancyStatus values follow a linear scale.
    /// Consumers should represent OccupancyStatus values as the state indicated
    /// and intended by the producer. Likewise, producers must use OccupancyStatus values that
    /// correspond to actual vehicle occupancy states.
    /// For describing passenger occupancy levels on a linear scale, see `occupancy_percentage`.
    /// This field is still experimental, and subject to change. It may be formally adopted in the future.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
    #[repr(i32)]
    pub enum OccupancyStatus {
        /// The vehicle or carriage is considered empty by most measures, and has few or no
        /// passengers onboard, but is still accepting passengers.
        Empty = 0,
        /// The vehicle or carriage has a large number of seats available.
        /// The amount of free seats out of the total seats available to be
        /// considered large enough to fall into this category is determined at the
        /// discretion of the producer.
        ManySeatsAvailable = 1,
        /// The vehicle or carriage has a relatively small number of seats available.
        /// The amount of free seats out of the total seats available to be
        /// considered small enough to fall into this category is determined at the
        /// discretion of the feed producer.
        FewSeatsAvailable = 2,
        /// The vehicle or carriage can currently accommodate only standing passengers.
        StandingRoomOnly = 3,
        /// The vehicle or carriage can currently accommodate only standing passengers
        /// and has limited space for them.
        CrushedStandingRoomOnly = 4,
        /// The vehicle or carriage is considered full by most measures, but may still be
        /// allowing passengers to board.
        Full = 5,
        /// The vehicle or carriage is not accepting passengers, but usually accepts passengers for boarding.
        NotAcceptingPassengers = 6,
        /// The vehicle or carriage doesn't have any occupancy data available at that time.
        NoDataAvailable = 7,
        /// The vehicle or carriage is not boardable and never accepts passengers.
        /// Useful for special vehicles or carriages (engine, maintenance carriage, etc…).
        NotBoardable = 8,
    }
}
/// An alert, indicating some sort of incident in the public transit network.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct Alert {
    /// Time when the alert should be shown to the user. If missing, the
    /// alert will be shown as long as it appears in the feed.
    /// If multiple ranges are given, the alert will be shown during all of them.
    pub active_period: Option<Many<TimeRange>>,
    /// Entities whose users we should notify of this alert.
    pub informed_entity: Option<Many<EntitySelector>>,

    pub cause: Option<alert::Cause>,
    pub effect: Option<alert::Effect>,
    /// The URL which provides additional information about the alert.
    pub url: Option<TranslatedString>,
    /// Alert header. Contains a short summary of the alert text as plain-text.
    pub header_text: Option<TranslatedString>,
    /// Full description for the alert as plain-text. The information in the
    /// description should add to the information of the header.
    pub description_text: Option<TranslatedString>,
    /// Text for alert header to be used in text-to-speech implementations. This field is the text-to-speech version of header_text.
    pub tts_header_text: Option<TranslatedString>,
    /// Text for full description for the alert to be used in text-to-speech implementations. This field is the text-to-speech version of description_text.
    pub tts_description_text: Option<TranslatedString>,
    pub severity_level: Option<alert::SeverityLevel>,
    /// TranslatedImage to be displayed along the alert text. Used to explain visually the alert effect of a detour, station closure, etc. The image must enhance the understanding of the alert. Any essential information communicated within the image must also be contained in the alert text.
    /// The following types of images are discouraged : image containing mainly text, marketing or branded images that add no additional information.
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub image: Option<TranslatedImage>,
    /// Text describing the appearance of the linked image in the `image` field (e.g., in case the image can't be displayed
    /// or the user can't see the image for accessibility reasons). See the HTML spec for alt image text - <https://html.spec.whatwg.org/#alt.>
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub image_alternative_text: Option<TranslatedString>,
    /// Description of the cause of the alert that allows for agency-specific language; more specific than the Cause. If cause_detail is included, then Cause must also be included.
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub cause_detail: Option<TranslatedString>,
    /// Description of the effect of the alert that allows for agency-specific language; more specific than the Effect. If effect_detail is included, then Effect must also be included.
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub effect_detail: Option<TranslatedString>,
}
/// Nested message and enum types in `Alert`.
pub mod alert {
    use serde::Deserialize;

    #[allow(clippy::enum_variant_names)]
    /// Cause of this alert. If cause_detail is included, then Cause must also be included.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum Cause {
        UnknownCause = 1,
        /// Not machine-representable.
        OtherCause = 2,
        TechnicalProblem = 3,
        /// Public transit agency employees stopped working.
        Strike = 4,
        /// People are blocking the streets.
        Demonstration = 5,
        Accident = 6,
        Holiday = 7,
        Weather = 8,
        Maintenance = 9,
        Construction = 10,
        PoliceActivity = 11,
        MedicalEmergency = 12,
    }

    /// What is the effect of this problem on the affected entity. If effect_detail is included, then Effect must also be included.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    #[allow(clippy::enum_variant_names)]
    pub enum Effect {
        NoService = 1,
        ReducedService = 2,
        /// We don't care about INsignificant delays: they are hard to detect, have
        /// little impact on the user, and would clutter the results as they are too
        /// frequent.
        SignificantDelays = 3,
        Detour = 4,
        AdditionalService = 5,
        ModifiedService = 6,
        OtherEffect = 7,
        UnknownEffect = 8,
        StopMoved = 9,
        NoEffect = 10,
        AccessibilityIssue = 11,
    }

    /// Severity of this alert.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum SeverityLevel {
        UnknownSeverity = 1,
        Info = 2,
        Warning = 3,
        Severe = 4,
    }
}
/// A time interval. The interval is considered active at time 't' if 't' is
/// greater than or equal to the start time and less than the end time.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct TimeRange {
    /// Start time, in POSIX time (i.e., number of seconds since January 1st 1970
    /// 00:00:00 UTC).
    /// If missing, the interval starts at minus infinity.
    pub start: Option<u64>,
    /// End time, in POSIX time (i.e., number of seconds since January 1st 1970
    /// 00:00:00 UTC).
    /// If missing, the interval ends at plus infinity.
    pub end: Option<u64>,
}
/// A position.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct Position {
    /// Degrees North, in the WGS-84 coordinate system.
    pub latitude: f32,
    /// Degrees East, in the WGS-84 coordinate system.
    pub longitude: f32,
    /// Bearing, in degrees, clockwise from North, i.e., 0 is North and 90 is East.
    /// This can be the compass bearing, or the direction towards the next stop
    /// or intermediate location.
    /// This should not be direction deduced from the sequence of previous
    /// positions, which can be computed from previous data.
    pub bearing: Option<MaybeStringWrapped<f32>>, // not supposed to be a string! But sometimes seems to be.
    /// Odometer value, in meters.
    pub odometer: Option<f64>,
    /// Momentary speed measured by the vehicle, in meters per second.
    pub speed: Option<f32>,
}
/// A descriptor that identifies an instance of a GTFS trip, or all instances of
/// a trip along a route.
/// - To specify a single trip instance, the trip_id (and if necessary,
///    start_time) is set. If route_id is also set, then it should be same as one
///    that the given trip corresponds to.
/// - To specify all the trips along a given route, only the route_id should be
///    set. Note that if the trip_id is not known, then stop sequence ids in
///    TripUpdate are not sufficient, and stop_ids must be provided as well. In
///    addition, absolute arrival/departure times must be provided.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct TripDescriptor {
    /// The trip_id from the GTFS feed that this selector refers to.
    /// For non frequency-based trips, this field is enough to uniquely identify
    /// the trip. For frequency-based trip, start_time and start_date might also be
    /// necessary. When schedule_relationship is DUPLICATED within a TripUpdate, the trip_id identifies the trip from
    /// static GTFS to be duplicated. When schedule_relationship is DUPLICATED within a VehiclePosition, the trip_id
    /// identifies the new duplicate trip and must contain the value for the corresponding TripUpdate.TripProperties.trip_id.
    pub trip_id: Option<String>,
    /// The route_id from the GTFS that this selector refers to.
    pub route_id: Option<String>,
    /// The direction_id from the GTFS feed trips.txt file, indicating the
    /// direction of travel for trips this selector refers to.
    pub direction_id: Option<u32>,
    /// The initially scheduled start time of this trip instance.
    /// When the trip_id corresponds to a non-frequency-based trip, this field
    /// should either be omitted or be equal to the value in the GTFS feed. When
    /// the trip_id correponds to a frequency-based trip, the start_time must be
    /// specified for trip updates and vehicle positions. If the trip corresponds
    /// to exact_times=1 GTFS record, then start_time must be some multiple
    /// (including zero) of headway_secs later than frequencies.txt start_time for
    /// the corresponding time period. If the trip corresponds to exact_times=0,
    /// then its start_time may be arbitrary, and is initially expected to be the
    /// first departure of the trip. Once established, the start_time of this
    /// frequency-based trip should be considered immutable, even if the first
    /// departure time changes -- that time change may instead be reflected in a
    /// StopTimeUpdate.
    /// Format and semantics of the field is same as that of
    /// GTFS/frequencies.txt/start_time, e.g., 11:15:35 or 25:15:35.
    pub start_time: Option<String>,
    /// The scheduled start date of this trip instance.
    /// Must be provided to disambiguate trips that are so late as to collide with
    /// a scheduled trip on a next day. For example, for a train that departs 8:00
    /// and 20:00 every day, and is 12 hours late, there would be two distinct
    /// trips on the same time.
    /// This field can be provided but is not mandatory for schedules in which such
    /// collisions are impossible - for example, a service running on hourly
    /// schedule where a vehicle that is one hour late is not considered to be
    /// related to schedule anymore.
    /// In YYYYMMDD format.
    pub start_date: Option<String>,

    pub schedule_relationship: Option<trip_descriptor::ScheduleRelationship>,
}
/// Nested message and enum types in `TripDescriptor`.
pub mod trip_descriptor {
    use serde_repr::Deserialize_repr;
    use serde_repr::Serialize_repr;

    /// The relation between this trip and the static schedule. If a trip is done
    /// in accordance with temporary schedule, not reflected in GTFS, then it
    /// shouldn't be marked as SCHEDULED, but likely as ADDED.
    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize_repr, Deserialize_repr,
    )]
    #[repr(i32)]
    pub enum ScheduleRelationship {
        /// Trip that is running in accordance with its GTFS schedule, or is close
        /// enough to the scheduled trip to be associated with it.
        Scheduled = 0,
        /// An extra trip that was added in addition to a running schedule, for
        /// example, to replace a broken vehicle or to respond to sudden passenger
        /// load.
        /// NOTE: Currently, behavior is unspecified for feeds that use this mode. There are discussions on the GTFS GitHub
        /// [(1)](<https://github.com/google/transit/issues/106>) [(2)](<https://github.com/google/transit/pull/221>)
        /// [(3)](<https://github.com/google/transit/pull/219>) around fully specifying or deprecating ADDED trips and the
        /// documentation will be updated when those discussions are finalized.
        Added = 1,
        /// A trip that is running with no schedule associated to it (GTFS frequencies.txt exact_times=0).
        /// Trips with ScheduleRelationship=UNSCHEDULED must also set all StopTimeUpdates.ScheduleRelationship=UNSCHEDULED.
        Unscheduled = 2,
        /// A trip that existed in the schedule but was removed.
        Canceled = 3,
        /// Should not be used - for backwards-compatibility only.
        Replacement = 5,
        /// An extra trip that was added in addition to a running schedule, for example, to replace a broken vehicle or to
        /// respond to sudden passenger load. Used with TripUpdate.TripProperties.trip_id, TripUpdate.TripProperties.start_date,
        /// and TripUpdate.TripProperties.start_time to copy an existing trip from static GTFS but start at a different service
        /// date and/or time. Duplicating a trip is allowed if the service related to the original trip in (CSV) GTFS
        /// (in calendar.txt or calendar_dates.txt) is operating within the next 30 days. The trip to be duplicated is
        /// identified via TripUpdate.TripDescriptor.trip_id. This enumeration does not modify the existing trip referenced by
        /// TripUpdate.TripDescriptor.trip_id - if a producer wants to cancel the original trip, it must publish a separate
        /// TripUpdate with the value of CANCELED or DELETED. Trips defined in GTFS frequencies.txt with exact_times that is
        /// empty or equal to 0 cannot be duplicated. The VehiclePosition.TripDescriptor.trip_id for the new trip must contain
        /// the matching value from TripUpdate.TripProperties.trip_id and VehiclePosition.TripDescriptor.ScheduleRelationship
        /// must also be set to DUPLICATED.
        /// Existing producers and consumers that were using the ADDED enumeration to represent duplicated trips must follow
        /// the migration guide (<https://github.com/google/transit/tree/master/gtfs-realtime/spec/en/examples/migration-duplicated.md>)
        /// to transition to the DUPLICATED enumeration.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        Duplicated = 6,
        /// A trip that existed in the schedule but was removed and must not be shown to users.
        /// DELETED should be used instead of CANCELED to indicate that a transit provider would like to entirely remove
        /// information about the corresponding trip from consuming applications, so the trip is not shown as cancelled to
        /// riders, e.g. a trip that is entirely being replaced by another trip.
        /// This designation becomes particularly important if several trips are cancelled and replaced with substitute service.
        /// If consumers were to show explicit information about the cancellations it would distract from the more important
        /// real-time predictions.
        /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
        Deleted = 7,
    }
}
/// Identification information for the vehicle performing the trip.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct VehicleDescriptor {
    /// Internal system identification of the vehicle. Should be unique per
    /// vehicle, and can be used for tracking the vehicle as it proceeds through
    /// the system.
    pub id: Option<String>,
    /// User visible label, i.e., something that must be shown to the passenger to
    /// help identify the correct vehicle.
    pub label: Option<String>,
    /// The license plate of the vehicle.
    pub license_plate: Option<String>,
    pub wheelchair_accessible: Option<vehicle_descriptor::WheelchairAccessible>,
}
/// Nested message and enum types in `VehicleDescriptor`.
pub mod vehicle_descriptor {
    use serde_repr::Deserialize_repr;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize_repr)]
    #[repr(i32)]
    #[allow(clippy::enum_variant_names)]
    pub enum WheelchairAccessible {
        /// The trip doesn't have information about wheelchair accessibility.
        /// This is the **default** behavior. If the static GTFS contains a
        /// _wheelchair_accessible_ value, it won't be overwritten.
        NoValue = 0,
        /// The trip has no accessibility value present.
        /// This value will overwrite the value from the GTFS.
        Unknown = 1,
        /// The trip is wheelchair accessible.
        /// This value will overwrite the value from the GTFS.
        WheelchairAccessible = 2,
        /// The trip is **not** wheelchair accessible.
        /// This value will overwrite the value from the GTFS.
        WheelchairInaccessible = 3,
    }
}
/// A selector for an entity in a GTFS feed.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct EntitySelector {
    /// The values of the fields should correspond to the appropriate fields in the
    /// GTFS feed.
    /// At least one specifier must be given. If several are given, then the
    /// matching has to apply to all the given specifiers.
    pub agency_id: Option<String>,

    pub route_id: Option<String>,
    /// corresponds to route_type in GTFS.
    pub route_type: Option<i32>,

    pub trip: Option<TripDescriptor>,

    pub stop_id: Option<String>,
    /// Corresponds to trip direction_id in GTFS trips.txt. If provided the
    /// route_id must also be provided.
    pub direction_id: Option<u32>,
}
/// An internationalized message containing per-language versions of a snippet of
/// text or a URL.
/// One of the strings from a message will be picked up. The resolution proceeds
/// as follows:
/// 1. If the UI language matches the language code of a translation,
///     the first matching translation is picked.
/// 2. If a default UI language (e.g., English) matches the language code of a
///     translation, the first matching translation is picked.
/// 3. If some translation has an unspecified language code, that translation is
///     picked.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct TranslatedString {
    /// At least one translation must be provided.
    pub translation: Option<Many<translated_string::Translation>>,
}

impl TranslatedString {
    pub fn get(&self, lang: &str) -> Option<String> {
        match &self.translation {
            Some(Many::One(t)) => Some(t.text.clone()),
            Some(Many::Many(v)) => {
                for t in v {
                    if let Some(l) = &t.language {
                        if l == lang {
                            return Some(t.text.clone());
                        }
                    }
                }
                None
            }
            None => None,
        }
    }
}

/// Nested message and enum types in `TranslatedString`.
pub mod translated_string {
    use serde::Deserialize;

    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct Translation {
        /// A UTF-8 string containing the message.
        pub text: String,
        /// BCP-47 language code. Can be omitted if the language is unknown or if
        /// no i18n is done at all for the feed. At most one translation is
        /// allowed to have an unspecified language tag.
        pub language: Option<String>,
    }
}
/// An internationalized image containing per-language versions of a URL linking to an image
/// along with meta information
/// Only one of the images from a message will be retained by consumers. The resolution proceeds
/// as follows:
/// 1. If the UI language matches the language code of a translation,
///     the first matching translation is picked.
/// 2. If a default UI language (e.g., English) matches the language code of a
///     translation, the first matching translation is picked.
/// 3. If some translation has an unspecified language code, that translation is
///     picked.
/// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct TranslatedImage {
    /// At least one localized image must be provided.
    pub localized_image: Option<Many<translated_image::LocalizedImage>>,
}
/// Nested message and enum types in `TranslatedImage`.
pub mod translated_image {
    use serde::Deserialize;

    #[derive(Clone, PartialEq, Deserialize, Debug)]
    pub struct LocalizedImage {
        /// String containing an URL linking to an image
        /// The image linked must be less than 2MB.
        /// If an image changes in a significant enough way that an update is required on the consumer side, the producer must update the URL to a new one.
        /// The URL should be a fully qualified URL that includes http:// or <https://,> and any special characters in the URL must be correctly escaped. See the following <http://www.w3.org/Addressing/URL/4_URI_Recommentations.html> for a description of how to create fully qualified URL values.
        pub url: String,
        /// IANA media type as to specify the type of image to be displayed.
        /// The type must start with "image/"
        pub media_type: String,
        /// BCP-47 language code. Can be omitted if the language is unknown or if
        /// no i18n is done at all for the feed. At most one translation is
        /// allowed to have an unspecified language tag.
        pub language: Option<String>,
    }
}
/// Describes the physical path that a vehicle takes when it's not part of the (CSV) GTFS,
/// such as for a detour. Shapes belong to Trips, and consist of a sequence of shape points.
/// Tracing the points in order provides the path of the vehicle.  Shapes do not need to intercept
/// the location of Stops exactly, but all Stops on a trip should lie within a small distance of
/// the shape for that trip, i.e. close to straight line segments connecting the shape points
/// NOTE: This message is still experimental, and subject to change. It may be formally adopted in the future.
#[derive(Clone, PartialEq, Deserialize, Debug)]
pub struct Shape {
    /// Identifier of the shape. Must be different than any shape_id defined in the (CSV) GTFS.
    /// This field is required as per reference.md, but needs to be specified here optional because "Required is Forever"
    /// See <https://developers.google.com/protocol-buffers/docs/proto#specifying_field_rules>
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub shape_id: Option<String>,
    /// Encoded polyline representation of the shape. This polyline must contain at least two points.
    /// For more information about encoded polylines, see <https://developers.google.com/maps/documentation/utilities/polylinealgorithm>
    /// This field is required as per reference.md, but needs to be specified here optional because "Required is Forever"
    /// See <https://developers.google.com/protocol-buffers/docs/proto#specifying_field_rules>
    /// NOTE: This field is still experimental, and subject to change. It may be formally adopted in the future.
    pub encoded_polyline: Option<String>,
}

#[cfg(test)]
mod test {

    #[test]
    fn test_deserialize_feed() {
        let msg = r#"{
            "header": {
                "timestamp": 1707115806.588,
                "gtfs_realtime_version": "1.0",
                "incrementality": 0
            },
            "entity": [
                {
                    "id": "1300-90602-68400-2-949b453b",
                    "trip_update": {
                        "trip": {
                            "trip_id": "1300-90602-68400-2-949b453b",
                            "start_time": "19:00:00",
                            "start_date": "20240205",
                            "schedule_relationship": 0,
                            "route_id": "906-203",
                            "direction_id": 1
                        },
                        "stop_time_update": {
                            "stop_sequence": 37,
                            "arrival": {
                                "delay": 106,
                                "time": 1707114886,
                                "uncertainty": 0
                            },
                            "stop_id": "4221-73f35c2b",
                            "schedule_relationship": 0
                        },
                        "vehicle": {
                            "id": "22781",
                            "label": "RT1349",
                            "license_plate": "LPM151"
                        },
                        "timestamp": 1707115459,
                        "delay": 106
                    },
                    "is_deleted": false
                },
                {
                    "id": "22781",
                    "vehicle": {
                        "trip": {
                            "trip_id": "1301-90702-71100-2-5759e324",
                            "start_time": "19:45:00",
                            "start_date": "20240205",
                            "schedule_relationship": 0,
                            "route_id": "907-203",
                            "direction_id": 1
                        },
                        "position": {
                            "latitude": -36.7612914,
                            "longitude": 174.7232526,
                            "bearing": "111",
                            "speed": 15
                        },
                        "timestamp": 1707115799,
                        "vehicle": {
                            "id": "22781",
                            "label": "RT1349",
                            "license_plate": "LPM151"
                        },
                        "occupancy_status": 1
                    },
                    "is_deleted": false
                }
            ]
        }"#;

        let feed: super::FeedMessage = serde_json::from_str(msg).unwrap();
        println!("{:#?}", feed);
    }
}
