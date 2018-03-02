--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE INDEX idx_e9bb5856__action_date_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_e9bb5856__type_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 100);
CREATE INDEX idx_e9bb5856__naics_temp ON summary_view_naics_codes_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_e9bb5856__tuned_type_and_idv_temp ON summary_view_naics_codes_temp USING BTREE("type", "pulled_from") WITH (fillfactor = 100) WHERE "type" IS NULL AND "pulled_from" IS NOT NULL;
