SELECT 
    t.trip_id, 
    st.stop_sequence,
    r.route_short_name, 
    st.stop_headsign, 
    start.departure_time AS trip_start_time, 
    st.arrival_time,
FROM 
    gtfs_stop_time st
LEFT JOIN 
    gtfs_stop_time start
    ON start.trip_id = st.trip_id 
    AND start.stop_sequence = 1
LEFT JOIN 
    gtfs_trip t
    ON st.trip_id = t.trip_id
LEFT JOIN 
    gtfs_route r
    ON r.route_id = t.route_id
LEFT JOIN 
    gtfs_calendar c
    ON t.service_id = c.service_id
LEFT JOIN 
    gtfs_calendar_date d
    ON d.service_id = c.service_id 
    AND d.date = ?
WHERE 
    st.stop_id = ?
    AND (st.departure_time >= ? OR st.arrival_time >= ?)
    AND (
        (
            -- regular services for the current day of week
            c.{{day_of_week}} = 1
            AND c.start_date <= ? AND c.end_date >= ? 
            -- except those that have been cancelled for this date
            AND (d.exception_type IS NULL OR d.exception_type <> 2)
        )
        OR
        (
            -- added services for this date
            d.date = ? AND d.exception_type = 1 
        )
    )
ORDER BY 
    st.arrival_time
LIMIT 10;
