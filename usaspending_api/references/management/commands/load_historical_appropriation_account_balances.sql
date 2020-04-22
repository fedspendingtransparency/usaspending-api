with
unioned as (
    select  alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct, tas_title,
            fr_entity, budget_subfunction, fy, pd,
            budgetary_resources_amount,
            0.00 deobligations_amount,
            0.00 obligations_amount,
            0.00 outlays_amount
    from    temp_load_historical_file_a_data_act_budgetary_resources

    union all

    select  alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct, tas_title,
            fr_entity, budget_subfunction, fy, pd,
            0.00 budgetary_resources_amount,
            deobligations_amount,
            0.00 obligations_amount,
            0.00 outlays_amount
    from    temp_load_historical_file_a_data_act_deobligations

    union all

    select  alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct, tas_title,
            fr_entity, budget_subfunction, fy, pd,
            0.00 budgetary_resources_amount,
            0.00 deobligations_amount,
            obligations_amount,
            0.00 outlays_amount
    from    temp_load_historical_file_a_data_act_obligations

    union all

    select  alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct, tas_title,
            fr_entity, budget_subfunction, fy, pd,
            0.00 budgetary_resources_amount,
            0.00 deobligations_amount,
            0.00 obligations_amount,
            outlays_amount
    from    temp_load_historical_file_a_data_act_outlays
),
grouped as (
    select  alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct,
            fr_entity, budget_subfunction, fy, pd,
            case when alloc_xfer_agency is null then '' else alloc_xfer_agency || '-' end ||
                agency_identifier || '-' ||
                case when avail_type_code is null then beg_poa || '/' || end_poa else avail_type_code end || '-' ||
                main_acct || '-' || sub_acct tas_rendering_label,
            case
                when pd in (1, 2, 3) then 1
                when pd in (4, 5, 6) then 2
                when pd in (7, 8, 9) then 3
                when pd in (10, 11, 12) then 4
            end quarter,
            case
                when pd in (1, 2, 3) then ((fy - 1) || '-10-01')::date
                when pd in (4, 5, 6) then ((fy) || '-01-01')::date
                when pd in (7, 8, 9) then ((fy) || '-04-01')::date
                when pd in (10, 11, 12) then ((fy) || '-07-01')::date
            end reporting_period_start,
            max(tas_title) tas_title,
            sum(budgetary_resources_amount) budgetary_resources_amount,
            sum(deobligations_amount) deobligations_amount,
            sum(obligations_amount) obligations_amount,
            sum(outlays_amount) outlays_amount
    from    unioned
    group   by alloc_xfer_agency, agency_identifier, avail_type_code,
            beg_poa, end_poa, main_acct, sub_acct,
            fr_entity, budget_subfunction, fy, pd
),
owning_agency as (
    -- This is remarkably similar to how we assign agencies for treasury_appropriation_accounts, however,
    -- it's just different enough to make reusing the same code challenging.  Since this is just a "one
    -- time load", we're just going to tweak the logic here.  If we make this a regular thing, look into
    -- consolidating the functions.
    select distinct on (g.agency_identifier, g.main_acct)
        g.agency_identifier,
        g.main_acct,
        coalesce(
            aid_cgac.toptier_agency_id,
            aid_frec.toptier_agency_id,
            aid_association.toptier_agency_id
        ) as owning_toptier_agency_id,
        coalesce(
            aid_cgac.toptier_code,
            aid_frec.toptier_code,
            aid_association.toptier_code
        )::int as toptier_code_sorter
    from
        grouped as g
        left outer join toptier_agency as aid_cgac on
            aid_cgac.toptier_code = case
                when g.agency_identifier in ('017', '021', '057') then '097'
                else g.agency_identifier
            end
        left outer join toptier_agency as aid_frec on
            aid_frec.toptier_code = g.fr_entity
        left outer join frec on
            frec.frec_code = g.fr_entity
        left outer join toptier_agency as aid_association on
            aid_association.toptier_code = frec.associated_cgac_code
    group by
        g.agency_identifier,
        g.main_acct,
        owning_toptier_agency_id,
        toptier_code_sorter
    order by
        g.agency_identifier,
        g.main_acct,
        count(*) desc,
        toptier_code_sorter
),
existing as (
    select  distinct
            t.tas_rendering_label,
            t.budget_subfunction_code,
            sa.reporting_fiscal_year,
            sa.reporting_fiscal_period
    from    submission_attributes sa
            inner join appropriation_account_balances b on sa.submission_id = b.submission_id
            inner join treasury_appropriation_account t on b.treasury_account_identifier = t.treasury_account_identifier
)
insert into historical_appropriation_account_balances (
        tas_rendering_label,
        allocation_transfer_agency_id,
        agency_id,
        beginning_period_of_availability,
        ending_period_of_availability,
        availability_type_code,
        main_account_code,
        sub_account_code,
        account_title,
        budget_function_code,
        budget_subfunction_code,
        fr_entity_code,
        total_budgetary_resources_amount_cpe,
        gross_outlay_amount_by_tas_cpe,
        deobligations_recoveries_refunds_by_tas_cpe,
        obligations_incurred_total_by_tas_cpe,
        reporting_period_start,
        reporting_period_end,
        reporting_fiscal_year,
        reporting_fiscal_quarter,
        reporting_fiscal_period,
        owning_toptier_agency_id,
        create_date,
        update_date
    )
    select
        g.tas_rendering_label,
        g.alloc_xfer_agency,
        g.agency_identifier,
        g.beg_poa,
        g.end_poa,
        g.avail_type_code,
        g.main_acct,
        g.sub_acct,
        g.tas_title,
        left(g.budget_subfunction, 2) || '0',
        g.budget_subfunction,
        g.fr_entity,
        coalesce(g.budgetary_resources_amount, 0.00),
        coalesce(g.outlays_amount, 0.00),
        coalesce(g.deobligations_amount, 0.00),
        coalesce(g.obligations_amount, 0.00),
        g.reporting_period_start,
        g.reporting_period_start + interval '3 months' - interval '1 day',
        g.fy,
        g.quarter,
        g.pd,
        oa.owning_toptier_agency_id,
        now(),
        now()
    from
        grouped g
        inner join owning_agency oa on
            oa.agency_identifier = g.agency_identifier and oa.main_acct = g.main_acct
        left outer join existing e on
            e.tas_rendering_label = g.tas_rendering_label and
            e.budget_subfunction_code = g.budget_subfunction and
            e.reporting_fiscal_year = g.fy and
            e.reporting_fiscal_period = g.pd and
            e.reporting_fiscal_year = 2017
    where
        -- As requested in the acceptance criteria for the ticket, we're excluding FY2013 and prior and
        -- anything after FY2017Q1.
        e.tas_rendering_label is null and
        (
            (g.fy > 2013 and g.fy < 2017) or
            (g.fy = 2017 and g.quarter < 2)
        )
