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
