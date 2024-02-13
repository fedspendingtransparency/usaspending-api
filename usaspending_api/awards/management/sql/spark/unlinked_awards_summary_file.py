summary_file = """
    with published_dabs_toptier_agency as (
        select distinct ta.toptier_code,
            ta.name
        from
            global_temp.toptier_agency ta
        inner join global_temp.agency a on
            (a.toptier_agency_id = ta.toptier_agency_id
                and a.toptier_flag = true)
        inner join global_temp.submission_attributes sa
        on
            ta.toptier_code = sa.toptier_code
        inner join global_temp.dabs_submission_window_schedule s on
            sa.submission_window_id = s.id
        where
            s.submission_reveal_date <= now()
    ),
    total_budgetary_resources as (
        select
            fiscal_year,
            fiscal_period,
            sum(total_budgetary_resources_cpe) as total_budgetary_resources
        from global_temp.gtas_sf133_balances gtas
        group by fiscal_year,
                fiscal_period
    )
    select
        pdta.name as agency_name,
        pdta.toptier_code as toptier_code,
        rao.fiscal_year,
        rao.fiscal_period,
        ((sum(rao.total_budgetary_resources) / min(tbr.total_budgetary_resources)) * 100) as percent_of_total_federal_budget,
        max(sa.published_date) as most_recent_update,
        count(rmt.tas_rendering_label) as missing_tas_accounts_count,
        sum(rao.total_diff_approp_ocpa_obligated_amounts) as reporting_difference_in_obligations,
        sum(rao.unlinked_procurement_c_awards + rao.unlinked_procurement_d_awards) as unlinked_contract_award_count,
        sum(rao.unlinked_assistance_c_awards + rao.unlinked_assistance_d_awards) as unlinked_assistance_award_count,
        sum(rao.unlinked_procurement_c_awards + rao.unlinked_procurement_d_awards) + sum(rao.unlinked_assistance_c_awards + rao.unlinked_assistance_d_awards) as total_unlinked_awards
    from published_dabs_toptier_agency as pdta

    inner join global_temp.reporting_agency_overview rao
        on rao.toptier_code = pdta.toptier_code

    left join total_budgetary_resources tbr
        on rao.fiscal_year = tbr.fiscal_year
        and rao.fiscal_period = tbr.fiscal_period

    left join global_temp.submission_attributes sa
        on sa.toptier_code = pdta.toptier_code
            and sa.reporting_fiscal_year = rao.fiscal_year
            and sa.reporting_fiscal_period = rao.fiscal_period

    left join global_temp.reporting_agency_missing_tas rmt
        on pdta.toptier_code = rmt.toptier_code
            and rao.fiscal_year = rmt.fiscal_year
            and rao.fiscal_period = rmt.fiscal_period
    group by
        pdta.name,
        pdta.toptier_code,
        rao.fiscal_year,
        rao.fiscal_period
"""
