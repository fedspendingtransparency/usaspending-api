--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_1b698194$f60_unique_pk_temp ON summary_view_cfda_number_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$f60_action_date_temp ON summary_view_cfda_number_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$f60_type_temp ON summary_view_cfda_number_temp USING BTREE("type") WITH (fillfactor = 97);
CREATE INDEX idx_1b698194$f60_pulled_from_temp ON summary_view_cfda_number_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
