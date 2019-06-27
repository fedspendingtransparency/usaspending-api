-- To prevent subaward boogers from accumulating, reset the subaward fields
-- in the awards table.  This is only run during a full refresh.



-- Get all award ids where we don't have a subaward and the subaward values
-- aren't already the defaults.
with reset_awards as (

    select
        a.id

    from
        awards a
        left outer join subaward s on s.award_id = a.id

    where
        s.id is null and (
            total_subaward_amount is not null or
            subaward_count is distinct from 0
        )

)
update
    awards

set
    total_subaward_amount = null,
    subaward_count = 0

from
    reset_awards ra

where
    awards.id = ra.id;
