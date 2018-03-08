--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
REFRESH MATERIALIZED VIEW CONCURRENTLY summary_transaction_geo_view WITH DATA;
ANALYZE VERBOSE summary_transaction_geo_view;
