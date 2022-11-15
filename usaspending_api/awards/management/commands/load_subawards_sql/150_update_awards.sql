-- Update counts in the awards table with new subaward totals.



with subaward_totals as (

    select
        award_id,
        sum(coalesce(amount, 0)) as total_subaward_amount,
        count(*) as subaward_count

    from
        subaward

    group by
        award_id

)
update
    award_search as a1

set
    total_subaward_amount = st.total_subaward_amount,
    subaward_count = coalesce(st.subaward_count, 0)

from
    vw_awards as a
    left outer join subaward_totals as st on st.award_id = a.id

where
    a.id = a1.id and (
        st.total_subaward_amount is distinct from a1.total_subaward_amount or
        coalesce(st.subaward_count, 0) is distinct from a1.subaward_count
    );
