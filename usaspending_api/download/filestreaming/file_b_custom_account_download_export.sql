-- Connections are pooled which means it's possible for temporary tables to survive beyond what we
-- would typically consider a "session".  Drop the temporary table just to be safe.
drop table if exists temp_file_b_custom_account_download_cleanup;


-- Add a row number (effectively a unique, surrogate id) to our normal download query and stuff it
-- into a temporary table.
create temporary table
    temp_file_b_custom_account_download_cleanup
as select
    row_number() over () as rownumber,
    t.*
from
    (
        {inner_sql}
    ) as t;


-- This significantly sped up subsequent queries for certain criteria. For example, the TAS query
-- for all of 2018 went from 1.5 minutes to 30 seconds.
create
index   temp_file_b_custom_account_download_cleanup_index
on      temp_file_b_custom_account_download_cleanup ({tas_or_fas});


-- "is distinct from" can be terribly inefficient.  As an example, the delete query further down
-- below took 45 seconds using "is (not) distinct from" and only 500 milliseconds using equals
-- for all TAS for 2018.  Since, in this context, nulls and ""s are functionally equivalent, let's
-- just replace all nulls with ""s on join columns to speed up queries.
update  temp_file_b_custom_account_download_cleanup

set     {tas_or_fas} = coalesce({tas_or_fas}, ''),
        object_class_code = coalesce(object_class_code, ''),
        program_activity_code = coalesce(program_activity_code, ''),
        budget_function = coalesce(budget_function, ''),
        budget_subfunction = coalesce(budget_subfunction, ''),
        last_reported_submission_period = coalesce(last_reported_submission_period, '')

where   {tas_or_fas} is null or
        object_class_code is null or
        program_activity_code is null or
        budget_function is null or
        budget_subfunction is null or
        last_reported_submission_period is null;


-- FOR FEDERAL ACCOUNTS ONLY, in cases where we have multiple records with the same
--
--   - {tas_or_fas}
--   - object_class_code
--   - program_activity_code
--   - budget_function
--   - budget_subfunction
--
-- and two flavors of direct_reimbursable where at least one is missing, combine all the rows
-- into one, summing their dollar figures and grabbing the highest last_reported_submission_period.
-- We will roll up values into one of the records that has a direct_or_reimbursable_funding_source
-- value to cut down on the number of columns we have to manipulate and delete all the other rows.
with

calculate_rollups as (
    select
        min(case when direct_or_reimbursable_funding_source is null then null else rownumber end) as rownumber,
        {tas_or_fas},
        object_class_code,
        program_activity_code,
        budget_function,
        budget_subfunction,
        max(last_reported_submission_period) as last_reported_submission_period,
        sum(obligations_incurred) as obligations_incurred,
        sum(deobligations_or_recoveries_or_refunds_from_prior_year) as deobligations_or_recoveries_or_refunds_from_prior_year,
        sum(gross_outlay_amount) as gross_outlay_amount
    from
        temp_file_b_custom_account_download_cleanup
    where
        '{tas_or_fas}' = 'federal_account_symbol'
    group by
        {tas_or_fas},
        object_class_code,
        program_activity_code,
        budget_function,
        budget_subfunction
    having
        array_length(array_agg(distinct direct_or_reimbursable_funding_source), 1) = 2 and
        array_position(array_agg(distinct direct_or_reimbursable_funding_source), null) is not null
),

update_rollups as (
    update
        temp_file_b_custom_account_download_cleanup as keeper
    set
        last_reported_submission_period = rollups.last_reported_submission_period,
        obligations_incurred = rollups.obligations_incurred,
        deobligations_or_recoveries_or_refunds_from_prior_year = rollups.deobligations_or_recoveries_or_refunds_from_prior_year,
        gross_outlay_amount = rollups.gross_outlay_amount
    from
        calculate_rollups as rollups
    where
        keeper.rownumber = rollups.rownumber
    returning
        rollups.rownumber,
        rollups.{tas_or_fas},
        rollups.object_class_code,
        rollups.program_activity_code,
        rollups.budget_function,
        rollups.budget_subfunction
)

delete
from    temp_file_b_custom_account_download_cleanup as t
using   update_rollups as r
where   t.{tas_or_fas} = r.{tas_or_fas} and
        t.object_class_code = r.object_class_code and
        t.program_activity_code = r.program_activity_code and
        t.budget_function = r.budget_function and
        t.budget_subfunction = r.budget_subfunction and
        t.rownumber != r.rownumber;


-- Delete $0 rows where the {tas_or_fas} in question is represented elsewhere with a
-- non $0 row.  A $0 row is one where all of the dollar figures are $0.
delete
from    temp_file_b_custom_account_download_cleanup as t
where   exists (
            select
            from    temp_file_b_custom_account_download_cleanup as n
            where   n.{tas_or_fas} = t.{tas_or_fas} and (
                        obligations_incurred != 0 or
                        deobligations_or_recoveries_or_refunds_from_prior_year != 0 or
                        gross_outlay_amount != 0
                    )
        ) and
        obligations_incurred = 0 and
        deobligations_or_recoveries_or_refunds_from_prior_year = 0 and
        gross_outlay_amount = 0;


-- At this point, the only $0 rows left are those that have no non $0 counterparts.  Sort
-- $0 rows and delete all but the first row for each {tas_or_fas} grouping.  The sort order
-- is specified in DEV-3997.
with

sorted_zero_sums as (
    select  rownumber,
            row_number() over (
                partition by    {tas_or_fas}
                order by        last_reported_submission_period desc,
                                object_class_code,
                                program_activity_code,
                                budget_function,
                                budget_subfunction,
                                direct_or_reimbursable_funding_source
            ) as ordinal
    from    temp_file_b_custom_account_download_cleanup
    where   obligations_incurred = 0 and
            deobligations_or_recoveries_or_refunds_from_prior_year = 0 and
            gross_outlay_amount = 0
)

delete
from    temp_file_b_custom_account_download_cleanup as t
where   exists(
            select
            from    sorted_zero_sums as s
            where   s.rownumber = t.rownumber and
                    s.ordinal > 1
        );


-- Remove the rownumber column as it is not part of the download file spec.
alter table temp_file_b_custom_account_download_cleanup drop column rownumber;


-- Export the results.
\copy (select * from temp_file_b_custom_account_download_cleanup order by last_reported_submission_period, {tas_or_fas}, object_class_code, program_activity_code, budget_function, budget_subfunction, direct_or_reimbursable_funding_source) to stdout {export_options};


drop table if exists temp_file_b_custom_account_download_cleanup;
