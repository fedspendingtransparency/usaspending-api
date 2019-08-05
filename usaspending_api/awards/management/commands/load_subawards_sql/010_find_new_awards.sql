-- Awards are added all the time.  Let's see if we can find any new
-- relationships.  If we do find a new relationship, uncheck the imported flag
-- in the broker_subaward table so that subaward gets picked up on the next run.



update
    broker_subaward

set
    imported = false

from
    subaward s
    inner join awards a on a.generated_unique_award_id = s.unique_award_key

where
    s.id = broker_subaward.id and
    s.award_id is null and
    s.unique_award_key is not null and
    broker_subaward.unique_award_key is not null;
