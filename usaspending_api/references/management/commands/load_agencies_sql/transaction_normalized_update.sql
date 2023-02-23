-- transaction_normalized records are linked to agencies when they're created.  This will fix any
-- incorrect links caused by changes to agencies.
update
    transaction_search as ts1

set
    awarding_agency_id = coalesce(aafabs.id, aafpds.id),
    funding_agency_id = coalesce(fafabs.id, fafpds.id)

from
    transaction_search as ts

    left outer join vw_transaction_fabs as fabs on fabs.transaction_id = ts.transaction_id
    left outer join subtier_agency as asafabs on asafabs.subtier_code = fabs.awarding_sub_tier_agency_c
    left outer join agency as aafabs on aafabs.subtier_agency_id = asafabs.subtier_agency_id
    left outer join subtier_agency as fsafabs on fsafabs.subtier_code = fabs.funding_sub_tier_agency_co
    left outer join agency as fafabs on fafabs.subtier_agency_id = fsafabs.subtier_agency_id

    left outer join vw_transaction_fpds as fpds on fpds.transaction_id = ts.transaction_id
    left outer join subtier_agency as asafpds on asafpds.subtier_code = fpds.awarding_sub_tier_agency_c
    left outer join agency as aafpds on aafpds.subtier_agency_id = asafpds.subtier_agency_id
    left outer join subtier_agency as fsafpds on fsafpds.subtier_code = fpds.funding_sub_tier_agency_co
    left outer join agency as fafpds on fafpds.subtier_agency_id = fsafpds.subtier_agency_id

where
    ts.transaction_id = ts1.transaction_id and (
        ts.awarding_agency_id is distinct from coalesce(aafabs.id, aafpds.id) or
        ts.funding_agency_id is distinct from coalesce(fafabs.id, fafpds.id)
    );
