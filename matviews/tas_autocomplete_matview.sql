DROP MATERIALIZED VIEW IF EXISTS tas_autocomplete_matview_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS tas_autocomplete_matview_old CASCADE;

CREATE MATERIALIZED VIEW tas_autocomplete_matview_temp AS
select
    min(taa.treasury_account_identifier) tas_autocomplete_id,
    taa.allocation_transfer_agency_id,
    taa.agency_id,
    taa.beginning_period_of_availability,
    taa.ending_period_of_availability,
    taa.availability_type_code,
    taa.main_account_code,
    taa.sub_account_code,
    taa.tas_rendering_label
from
    treasury_appropriation_account taa
    inner join financial_accounts_by_awards faba on taa.treasury_account_identifier = faba.treasury_account_id
where
    faba.award_id is not null
group by
    taa.allocation_transfer_agency_id,
    taa.agency_id,
    taa.beginning_period_of_availability,
    taa.ending_period_of_availability,
    taa.availability_type_code,
    taa.main_account_code,
    taa.sub_account_code,
    taa.tas_rendering_label
order by
    taa.main_account_code, taa.agency_id WITH DATA;

CREATE UNIQUE INDEX idx_17cb5682$aa5_tas_autocomplete_id_temp ON tas_autocomplete_matview_temp USING BTREE(tas_autocomplete_id) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_allocation_transfer_agency_id_temp ON tas_autocomplete_matview_temp USING BTREE(allocation_transfer_agency_id) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_agency_id_temp ON tas_autocomplete_matview_temp USING BTREE(agency_id) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_beginning_period_of_availability_temp ON tas_autocomplete_matview_temp USING BTREE(beginning_period_of_availability) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_ending_period_of_availability_temp ON tas_autocomplete_matview_temp USING BTREE(ending_period_of_availability) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_availability_type_code_temp ON tas_autocomplete_matview_temp USING BTREE(availability_type_code) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_main_account_code_temp ON tas_autocomplete_matview_temp USING BTREE(main_account_code) WITH (fillfactor = 97);
CREATE INDEX idx_17cb5682$aa5_sub_account_code_temp ON tas_autocomplete_matview_temp USING BTREE(sub_account_code) WITH (fillfactor = 97);


ALTER MATERIALIZED VIEW IF EXISTS tas_autocomplete_matview RENAME TO tas_autocomplete_matview_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_tas_autocomplete_id RENAME TO idx_17cb5682$aa5_tas_autocomplete_id_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_allocation_transfer_agency_id RENAME TO idx_17cb5682$aa5_allocation_transfer_agency_id_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_agency_id RENAME TO idx_17cb5682$aa5_agency_id_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_beginning_period_of_availability RENAME TO idx_17cb5682$aa5_beginning_period_of_availability_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_ending_period_of_availability RENAME TO idx_17cb5682$aa5_ending_period_of_availability_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_availability_type_code RENAME TO idx_17cb5682$aa5_availability_type_code_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_main_account_code RENAME TO idx_17cb5682$aa5_main_account_code_old;
ALTER INDEX IF EXISTS idx_17cb5682$aa5_sub_account_code RENAME TO idx_17cb5682$aa5_sub_account_code_old;


ALTER MATERIALIZED VIEW tas_autocomplete_matview_temp RENAME TO tas_autocomplete_matview;
ALTER INDEX idx_17cb5682$aa5_tas_autocomplete_id_temp RENAME TO idx_17cb5682$aa5_tas_autocomplete_id;
ALTER INDEX idx_17cb5682$aa5_allocation_transfer_agency_id_temp RENAME TO idx_17cb5682$aa5_allocation_transfer_agency_id;
ALTER INDEX idx_17cb5682$aa5_agency_id_temp RENAME TO idx_17cb5682$aa5_agency_id;
ALTER INDEX idx_17cb5682$aa5_beginning_period_of_availability_temp RENAME TO idx_17cb5682$aa5_beginning_period_of_availability;
ALTER INDEX idx_17cb5682$aa5_ending_period_of_availability_temp RENAME TO idx_17cb5682$aa5_ending_period_of_availability;
ALTER INDEX idx_17cb5682$aa5_availability_type_code_temp RENAME TO idx_17cb5682$aa5_availability_type_code;
ALTER INDEX idx_17cb5682$aa5_main_account_code_temp RENAME TO idx_17cb5682$aa5_main_account_code;
ALTER INDEX idx_17cb5682$aa5_sub_account_code_temp RENAME TO idx_17cb5682$aa5_sub_account_code;


ANALYZE VERBOSE tas_autocomplete_matview;
GRANT SELECT ON tas_autocomplete_matview TO readonly;
