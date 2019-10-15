-- treasury_appropriation_accounts are linked to toptier agencies when they're created.  This will fix any
-- incorrect links caused by changes to agencies.
update
    treasury_appropriation_account as taa1
set
    awarding_toptier_agency_id = ata.toptier_agency_id,
    funding_toptier_agency_id = coalesce(aid_cgac.toptier_agency_id, aid_frec.toptier_agency_id)
from
    treasury_appropriation_account as taa
    left outer join toptier_agency as ata on ata.cgac_code = taa.allocation_transfer_agency_id
    left outer join toptier_agency as aid_cgac on aid_cgac.cgac_code = taa.agency_id
    left outer join toptier_agency as aid_frec on aid_frec.cgac_code = taa.fr_entity_code
where
    taa.treasury_account_identifier = taa1.treasury_account_identifier and (
        taa.awarding_toptier_agency_id is distinct from ata.toptier_agency_id or
        taa.funding_toptier_agency_id is distinct from coalesce(aid_cgac.toptier_agency_id, aid_frec.toptier_agency_id)
    );
