CREATE UNIQUE INDEX idx_21c979c6$49d_transaction_id_temp ON transaction_search_temp USING BTREE(transaction_id ASC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_21c979c6$49d_fiscal_year_temp ON transaction_search_temp USING BTREE(fiscal_year DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_pop_zip5_temp ON transaction_search_temp USING BTREE(pop_zip5) WITH (fillfactor = 97) WHERE pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_simple_pop_geolocation_temp ON transaction_search_temp USING BTREE(pop_state_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_etl_update_date_temp ON transaction_search_temp USING BTREE(etl_update_date) WITH (fillfactor = 97);
