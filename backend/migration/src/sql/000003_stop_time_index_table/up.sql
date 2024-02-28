CREATE TABLE "stop_time_index" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "stop_id" TEXT NOT NULL,
    "stop_sequence" INTEGER NOT NULL,
    "trip_id" TEXT NOT NULL,
    "trip_run_id" BIGINT NOT NULL,
    "arrival_timestamp" BIGINT NOT NULL,
    "departure_timestamp" BIGINT NOT NULL,
    "updated_arrival_timestamp" BIGINT,
    FOREIGN KEY ("stop_id", "stop_sequence", "trip_id") REFERENCES "gtfs_stop_times" ("stop_id", "stop_sequence", "trip_id"),
    FOREIGN KEY ("trip_run_id") REFERENCES "trip_run" ("id")
);
