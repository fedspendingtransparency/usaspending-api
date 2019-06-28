-- To prevent subaward boogers from accumulating, reset the subaward fields
-- in the awards table.  This is only run during a full refresh.



-- Get all award ids where we don't have a subaward and the subaward values
-- aren't already the defaults.
update
    awards

set
    total_subaward_amount = null,
    subaward_count = 0

where
    id not in (select award_id from subaward where award_id is not null) and (
        total_subaward_amount is not null or
        subaward_count is distinct from 0
    );
