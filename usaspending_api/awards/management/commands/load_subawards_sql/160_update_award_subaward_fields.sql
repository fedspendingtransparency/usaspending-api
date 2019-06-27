-- Update counts in the awards table with new subaward totals.



with subaward_totals as (

    select
        award_id,
        sum(amount) as total_subaward_amount,
        count(*) as subaward_count

    from
        subaward

    {where}

    group by
        award_id

)
update
    awards

set
    total_subaward_amount = coalesce(st.total_subaward_amount, 0),
    subaward_count = st.subaward_count

from
    subaward_totals st

where
    st.award_id = awards.id and (
        st.total_subaward_amount is distinct from awards.total_subaward_amount or
        st.subaward_count is distinct from awards.subaward_count
    );
