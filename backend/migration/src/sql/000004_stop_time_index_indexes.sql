CREATE INDEX "idx_sti_stop_id" ON "stop_time_index" ("stop_id");
CREATE INDEX "idx_sti_trip_id" ON "stop_time_index" ("trip_id");
CREATE INDEX "idx_sti_trip_run_id" ON "stop_time_index" ("trip_run_id");
CREATE INDEX "idx_sti_arrival_timestamp" ON "stop_time_index" ("arrival_timestamp");
CREATE INDEX "idx_sti_updated_timestamp" ON "stop_time_index" ("updated_arrival_timestamp");
