CREATE INDEX idx_adhoc$811_compound_geo_pop_1 ON universal_transaction_matview USING BTREE(pop_state_code, pop_county_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND action_date >= '2007-10-01';
CREATE INDEX idx_adhoc$811_compound_geo_pop_2 ON universal_transaction_matview USING BTREE(pop_state_code, pop_congressional_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND action_date >= '2007-10-01';
CREATE INDEX idx_adhoc$811_compound_geo_pop_3 ON universal_transaction_matview USING BTREE(pop_zip5, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND action_date >= '2007-10-01';
CREATE INDEX idx_adhoc$811_compound_geo_rl_1 ON universal_transaction_matview USING BTREE(recipient_location_state_code, recipient_location_county_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND action_date >= '2007-10-01';
CREATE INDEX idx_adhoc$811_compound_geo_rl_2 ON universal_transaction_matview USING BTREE(recipient_location_state_code, recipient_location_congressional_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND action_date >= '2007-10-01';
CREATE INDEX idx_adhoc$811_compound_geo_rl_3 ON universal_transaction_matview USING BTREE(recipient_location_zip5, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND action_date >= '2007-10-01';

CREATE INDEX idx_adhoc$e18_compound_geo_pop_1 ON summary_transaction_geo_view USING BTREE(pop_state_code, pop_county_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$e18_compound_geo_pop_2 ON summary_transaction_geo_view USING BTREE(pop_state_code, pop_congressional_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$e18_compound_geo_pop_3 ON summary_transaction_geo_view USING BTREE(pop_zip5, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$e18_compound_geo_rl_1 ON summary_transaction_geo_view USING BTREE(recipient_location_state_code, recipient_location_county_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$e18_compound_geo_rl_2 ON summary_transaction_geo_view USING BTREE(recipient_location_state_code, recipient_location_congressional_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$e18_compound_geo_rl_3 ON summary_transaction_geo_view USING BTREE(recipient_location_zip5, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';

CREATE INDEX idx_adhoc$997_compound_geo_pop_1 ON summary_transaction_month_view USING BTREE(pop_state_code, pop_county_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$997_compound_geo_pop_2 ON summary_transaction_month_view USING BTREE(pop_state_code, pop_congressional_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$997_compound_geo_pop_3 ON summary_transaction_month_view USING BTREE(pop_zip5, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$997_compound_geo_rl_1 ON summary_transaction_month_view USING BTREE(recipient_location_state_code, recipient_location_county_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$997_compound_geo_rl_2 ON summary_transaction_month_view USING BTREE(recipient_location_state_code, recipient_location_congressional_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$997_compound_geo_rl_3 ON summary_transaction_month_view USING BTREE(recipient_location_zip5, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';

CREATE INDEX idx_adhoc$fc1_compound_geo_pop_1 ON summary_transaction_view USING BTREE(pop_state_code, pop_county_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$fc1_compound_geo_pop_2 ON summary_transaction_view USING BTREE(pop_state_code, pop_congressional_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$fc1_compound_geo_pop_3 ON summary_transaction_view USING BTREE(pop_zip5, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA';
CREATE INDEX idx_adhoc$fc1_compound_geo_rl_1 ON summary_transaction_view USING BTREE(recipient_location_state_code, recipient_location_county_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$fc1_compound_geo_rl_2 ON summary_transaction_view USING BTREE(recipient_location_state_code, recipient_location_congressional_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';
CREATE INDEX idx_adhoc$fc1_compound_geo_rl_3 ON summary_transaction_view USING BTREE(recipient_location_zip5, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA';

DROP INDEX idx_7994619c$30b_compound_geo_pop_1; -- summary_transaction_geo_view
DROP INDEX idx_7994619c$30b_compound_geo_pop_2; -- summary_transaction_geo_view
DROP INDEX idx_7994619c$30b_compound_geo_pop_3; -- summary_transaction_geo_view
DROP INDEX idx_7994619c$30b_compound_geo_rl_1; -- summary_transaction_geo_view
DROP INDEX idx_7994619c$30b_compound_geo_rl_2; -- summary_transaction_geo_view
DROP INDEX idx_7994619c$30b_compound_geo_rl_3; -- summary_transaction_geo_view
DROP INDEX idx_c0be016a$36c_compound_geo_pop_1; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$36c_compound_geo_pop_2; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$36c_compound_geo_pop_3; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$36c_compound_geo_rl_1; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$36c_compound_geo_rl_2; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$36c_compound_geo_rl_3; -- summary_transaction_month_view
DROP INDEX idx_c0be016a$a87_compound_geo_pop_1; -- summary_transaction_view
DROP INDEX idx_c0be016a$a87_compound_geo_pop_2; -- summary_transaction_view
DROP INDEX idx_c0be016a$a87_compound_geo_pop_3; -- summary_transaction_view
DROP INDEX idx_c0be016a$a87_compound_geo_rl_1; -- summary_transaction_view
DROP INDEX idx_c0be016a$a87_compound_geo_rl_2; -- summary_transaction_view
DROP INDEX idx_c0be016a$a87_compound_geo_rl_3; -- summary_transaction_view
DROP INDEX idx_c0be016a$4c1_compound_geo_pop_1; -- universal_transaction_matview
DROP INDEX idx_c0be016a$4c1_compound_geo_pop_2; -- universal_transaction_matview
DROP INDEX idx_c0be016a$4c1_compound_geo_pop_3; -- universal_transaction_matview
DROP INDEX idx_c0be016a$4c1_compound_geo_rl_1; -- universal_transaction_matview
DROP INDEX idx_c0be016a$4c1_compound_geo_rl_2; -- universal_transaction_matview
DROP INDEX idx_c0be016a$4c1_compound_geo_rl_3; -- universal_transaction_matview