CREATE INDEX idx_17cb5682$aa5_agency_id_temp ON tas_autocomplete_matview_temp USING BTREE(agency_id) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_availability_type_code_temp ON tas_autocomplete_matview_temp USING BTREE(availability_type_code) WITH (fillfactor = 97);
