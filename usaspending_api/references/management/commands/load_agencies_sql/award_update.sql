-- awards are linked to agencies when they're created and when new transactions are created for them.
-- This will fix any incorrect links caused by changes to agencies.
update
    award_search as a
set
    awarding_agency_id = tn.awarding_agency_id,
    funding_agency_id = tn.funding_agency_id
from
    vw_transaction_normalized as tn
where
    tn.id = a.latest_transaction_id and (
        tn.awarding_agency_id is distinct from a.awarding_agency_id or
        tn.funding_agency_id is distinct from a.funding_agency_id
    );
