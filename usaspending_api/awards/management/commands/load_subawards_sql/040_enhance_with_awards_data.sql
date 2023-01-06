-- Add award data into subawards.



update
    temp_load_subawards_subaward

set
    award_id = a.id,
    awarding_agency_id = a.awarding_agency_id,
    funding_agency_id = a.funding_agency_id,
    last_modified_date = a.last_modified_date,
    latest_transaction_id = a.latest_transaction_id,
    prime_award_type = a.type,
    business_categories = coalesce(tn.business_categories, '{}'::text[])

from
    vw_awards a
    left outer join transaction_normalized tn on tn.id = a.latest_transaction_id

where
    a.generated_unique_award_id = temp_load_subawards_subaward.unique_award_key and
    temp_load_subawards_subaward.unique_award_key is not null;
