CREATE INDEX idx_17cb5682$aa5_allocation_transfer_agency_id_temp ON tas_autocomplete_matview_temp USING BTREE(allocation_transfer_agency_id) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_ending_period_of_availability_temp ON tas_autocomplete_matview_temp USING BTREE(ending_period_of_availability) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_sub_account_code_temp ON tas_autocomplete_matview_temp USING BTREE(sub_account_code) WITH (fillfactor = 97);
