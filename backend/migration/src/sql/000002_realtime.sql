
-- Represents a specific run of a trip at a specific time
-- This is loosely based on a "TripDescriptor" in the realtime spec
CREATE TABLE "trip_run" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "trip_id" TEXT NOT NULL,
    "route_id" TEXT NOT NULL,
    "direction_id" INTEGER,
    -- The scheduled date of the trip, which could be the previous day
    -- this aids in searching from a TripDescriptor
    "start_date" TEXT NOT NULL,
    "start_timestamp" BIGINT NOT NULL,
    "schedule_relationship" INTEGER NOT NULL DEFAULT 0,
    -- vehicle assigned to this trip if known
    "vehicle_id" TEXT,
    UNIQUE ("trip_id", "start_timestamp"),
    FOREIGN KEY ("trip_id") REFERENCES "gtfs_trips" ("trip_id"),
    FOREIGN KEY ("route_id") REFERENCES "gtfs_routes" ("route_id"),
    FOREIGN KEY ("vehicle_id") REFERENCES "vehicle" ("vehicle_id")
);

CREATE INDEX "idx_tr_day_route" ON "trip_run" ("route_id", "schedule_date");

CREATE TABLE "alert" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "alert_id" TEXT UNIQUE,
    "cause" INTEGER,
    "effect" INTEGER,
    "header_text" TEXT,
    "description_text" TEXT,
    "timestamp" BIGINT
);

CREATE TABLE "alert_active_period" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "alert_id" TEXT NOT NULL,
    "start_timestamp" BIGINT NOT NULL,
    "end_timestamp" BIGINT NOT NULL,
    FOREIGN KEY ("alert_id") REFERENCES "alert" ("alert_id") ON DELETE CASCADE
);

CREATE TABLE "alert_informed_entity" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "alert_id" TEXT,
    "agency_id" TEXT,
    "route_id" TEXT,
    "route_type" INTEGER,
    "direction_id" INTEGER,
    "stop_id" TEXT,
    "trip_run_id" BIGINT,
    FOREIGN KEY ("alert_id") REFERENCES "alert" ("alert_id") ON DELETE CASCADE,
    FOREIGN KEY ("agency_id") REFERENCES "gtfs_agency" ("agency_id") ON DELETE CASCADE,
    FOREIGN KEY ("route_id") REFERENCES "gtfs_routes" ("route_id") ON DELETE CASCADE,
    FOREIGN KEY ("trip_run_id") REFERENCES "trip_run" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("stop_id") REFERENCES "gtfs_stops" ("stop_id") ON DELETE CASCADE
);

CREATE TABLE "vehicle" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "vehicle_id" TEXT NOT NULL UNIQUE,
    "timestamp" BIGINT NOT NULL,
    "label" TEXT,
    "license_plate" TEXT,
    "latitude" REAL,
    "longitude" REAL,
    "bearing" REAL,
    "speed" REAL,
    "occupancy_status" INTEGER
);

CREATE INDEX "idx_vehicle_timestamp" ON "vehicle" ("timestamp");
