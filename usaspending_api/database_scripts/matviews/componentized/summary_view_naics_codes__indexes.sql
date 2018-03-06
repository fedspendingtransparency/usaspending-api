--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_af8ca7ca$50d_unique_pk_temp ON summary_view_naics_codes_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$50d_action_date_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$50d_type_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$50d_naics_temp ON summary_view_naics_codes_temp USING BTREE("naics_code") WITH (fillfactor = 97) WHERE "naics_code" IS NOT NULL;
