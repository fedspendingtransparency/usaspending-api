/*
Jira Ticket Number(s): DEV-3759

Expected CLI:

    $ psql ${DATABASE_URL} -a -f usaspending_api/database_scripts/job_archive/fix_awards_earliest_latest_transactions_and_exec_comp.sql

Purpose:

    Update awards where the earliest transaction, latest transaction, or officers is incorrect
    according to new sorting algorithm.

Life expectancy:

    Once Sprint 94 has been rolled out to production this script is safe to delete... although I
    would recommend keeping it around for a few additional sprints for reference.

*/
with

earliest_transaction as (
    select distinct on (award_id)
        award_id,
        id as transaction_id
    from
        transaction_normalized
    order by
        award_id,
        action_date asc,
        modification_number asc,
        transaction_unique_id asc
),

latest_transaction as (
    select distinct on (award_id)
        award_id,
        id as transaction_id
    from
        transaction_normalized
    order by
        award_id,
        action_date desc,
        modification_number desc,
        transaction_unique_id desc
),

executive_compensation as (
    select distinct on (award_id)
        tn.award_id,
        coalesce(fabs.officer_1_amount, fpds.officer_1_amount) as officer_1_amount,
        coalesce(fabs.officer_1_name, fpds.officer_1_name) as officer_1_name,
        coalesce(fabs.officer_2_amount, fpds.officer_2_amount) as officer_2_amount,
        coalesce(fabs.officer_2_name, fpds.officer_2_name) as officer_2_name,
        coalesce(fabs.officer_3_amount, fpds.officer_3_amount) as officer_3_amount,
        coalesce(fabs.officer_3_name, fpds.officer_3_name) as officer_3_name,
        coalesce(fabs.officer_4_amount, fpds.officer_4_amount) as officer_4_amount,
        coalesce(fabs.officer_4_name, fpds.officer_4_name) as officer_4_name,
        coalesce(fabs.officer_5_amount, fpds.officer_5_amount) as officer_5_amount,
        coalesce(fabs.officer_5_name, fpds.officer_5_name) as officer_5_name
    from
        transaction_normalized as tn
        left outer join transaction_fabs fabs on fabs.transaction_id = tn.id
        left outer join transaction_fpds fpds on fpds.transaction_id = tn.id
    where
        coalesce(fabs.officer_1_name, fpds.officer_1_name) is not null
    order by
        tn.award_id,
        tn.action_date desc,
        tn.modification_number desc,
        tn.transaction_unique_id desc
),

requiring_update as (
    select
        a.id as award_id,
        e.transaction_id as earliest_transaction_id,
        l.transaction_id as latest_transaction_id,
        ec.officer_1_amount,
        ec.officer_1_name,
        ec.officer_2_amount,
        ec.officer_2_name,
        ec.officer_3_amount,
        ec.officer_3_name,
        ec.officer_4_amount,
        ec.officer_4_name,
        ec.officer_5_amount,
        ec.officer_5_name
    from
        awards as a
        left outer join earliest_transaction e on e.award_id = a.id
        left outer join latest_transaction l on l.award_id = a.id
        left outer join executive_compensation ec on ec.award_id = a.id
    where
        a.earliest_transaction_id is distinct from e.transaction_id or
        a.latest_transaction_id is distinct from l.transaction_id or
        a.officer_1_amount is distinct from ec.officer_1_amount or
        a.officer_1_name is distinct from ec.officer_1_name or
        a.officer_2_amount is distinct from ec.officer_2_amount or
        a.officer_2_name is distinct from ec.officer_2_name or
        a.officer_3_amount is distinct from ec.officer_3_amount or
        a.officer_3_name is distinct from ec.officer_3_name or
        a.officer_4_amount is distinct from ec.officer_4_amount or
        a.officer_4_name is distinct from ec.officer_4_name or
        a.officer_5_amount is distinct from ec.officer_5_amount or
        a.officer_5_name is distinct from ec.officer_5_name
)

update
    awards as a
set
    earliest_transaction_id = e.id,
    date_signed = e.action_date,
    description = e.description,
    period_of_performance_start_date = e.period_of_performance_start_date,
    latest_transaction_id = l.id,
    awarding_agency_id = l.awarding_agency_id,
    category = case
        when l.type in ('A', 'B', 'C', 'D') then 'contract'
        when l.type in ('02', '03', '04', '05') then 'grant'
        when l.type in ('06', '10') then 'direct payment'
        when l.type in ('07', '08') then 'loans'
        when l.type = '09' then 'insurance'
        when l.type = '11' then 'other'
        when l.type like 'IDV%%' then 'idv'
        else null
    end,
    certified_date = l.action_date,
    funding_agency_id = l.funding_agency_id,
    last_modified_date = l.last_modified_date,
    period_of_performance_current_end_date = l.period_of_performance_current_end_date,
    place_of_performance_id = l.place_of_performance_id,
    recipient_id = l.recipient_id,
    type = l.type,
    type_description = l.type_description,
    officer_1_amount = r.officer_1_amount,
    officer_1_name = r.officer_1_name,
    officer_2_amount = r.officer_2_amount,
    officer_2_name = r.officer_2_name,
    officer_3_amount = r.officer_3_amount,
    officer_3_name = r.officer_3_name,
    officer_4_amount = r.officer_4_amount,
    officer_4_name = r.officer_4_name,
    officer_5_amount = r.officer_5_amount,
    officer_5_name = r.officer_5_name
from
    requiring_update r
    left outer join transaction_normalized e on e.id = r.earliest_transaction_id
    left outer join transaction_normalized l on l.id = r.latest_transaction_id
where
    a.id = r.award_id;
