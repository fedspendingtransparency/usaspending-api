--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE INDEX idx_3a7bb23c$263__action_date_temp ON summary_award_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$263__type_temp ON summary_award_view_temp USING BTREE("type") WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$263__fy_temp ON summary_award_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$263__pulled_from_temp ON summary_award_view_temp USING BTREE("pulled_from") WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__awarding_agency_id_temp ON summary_award_view_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__funding_agency_id_temp ON summary_award_view_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__awarding_toptier_agency_name_temp ON summary_award_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 100) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__awarding_subtier_agency_name_temp ON summary_award_view_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 100) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__funding_toptier_agency_name_temp ON summary_award_view_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 100) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__funding_subtier_agency_name_temp ON summary_award_view_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 100) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$263__awarding_toptier_agency_ts_vector_temp ON summary_award_view_temp USING GIN("awarding_toptier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$263__awarding_subtier_agency_ts_vector_temp ON summary_award_view_temp USING GIN("awarding_subtier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$263__funding_toptier_agency_ts_vector_temp ON summary_award_view_temp USING GIN("funding_toptier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$263__funding_subtier_agency_ts_vector_temp ON summary_award_view_temp USING GIN("funding_subtier_agency_ts_vector");
