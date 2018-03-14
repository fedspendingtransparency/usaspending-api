--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_1b698194$ca7_unique_pk_temp ON summary_view_naics_codes_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$ca7_action_date_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$ca7_type_temp ON summary_view_naics_codes_temp USING BTREE("type") WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$ca7_naics_temp ON summary_view_naics_codes_temp USING BTREE("naics_code") WITH (fillfactor = 97) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_1b698194$ca7_pulled_from_temp ON summary_view_naics_codes_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
