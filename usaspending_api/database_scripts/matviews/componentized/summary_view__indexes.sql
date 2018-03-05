--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_cb0c2fed$cac_unique_pk_temp ON summary_view_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_cb0c2fed$cac_action_date_temp ON summary_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cb0c2fed$cac_type_temp ON summary_view_temp USING BTREE("type") WITH (fillfactor = 97);
CREATE INDEX idx_cb0c2fed$cac_fy_temp ON summary_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cb0c2fed$cac_pulled_from_temp ON summary_view_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_awarding_agency_id_temp ON summary_view_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_funding_agency_id_temp ON summary_view_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_awarding_toptier_agency_name_temp ON summary_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_awarding_subtier_agency_name_temp ON summary_view_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_funding_toptier_agency_name_temp ON summary_view_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_cb0c2fed$cac_funding_subtier_agency_name_temp ON summary_view_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;
