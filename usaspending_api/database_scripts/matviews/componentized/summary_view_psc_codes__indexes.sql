--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_af8ca7ca$677_unique_pk_temp ON summary_view_psc_codes_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$677_action_date_temp ON summary_view_psc_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$677_type_temp ON summary_view_psc_codes_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 97);
