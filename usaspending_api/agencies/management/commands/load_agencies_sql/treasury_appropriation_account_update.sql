-- treasury_appropriation_accounts are linked to toptier agencies when they're created.  This will fix any
-- incorrect links caused by changes to agencies.
update
    treasury_appropriation_account as taa1
set
    awarding_toptier_agency_id = ta.toptier_agency_id,
    funding_toptier_agency_id = coalesce(cta.toptier_agency_id, fta.toptier_agency_id)
from
    treasury_appropriation_account as taa
    left outer join toptier_agency as ta on ta.cgac_code = taa.allocation_transfer_agency_id
    left outer join toptier_agency as cta on cta.cgac_code = taa.agency_id
    left outer join toptier_agency as fta on fta.cgac_code = taa.fr_entity_code
where
    taa.treasury_account_identifier = taa1.treasury_account_identifier and (
        taa.awarding_toptier_agency_id is distinct from ta.toptier_agency_id or
        taa.funding_toptier_agency_id is distinct from coalesce(cta.toptier_agency_id, fta.toptier_agency_id)
    );
