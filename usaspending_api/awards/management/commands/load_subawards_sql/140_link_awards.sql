-- Awards are added all the time.  Let's see if we can find any new relationships.



update
    subaward as s

set
    award_id = a.id

from
    vw_awards as a

where
    a.generated_unique_award_id = s.unique_award_key and
    s.award_id is distinct from a.id;
