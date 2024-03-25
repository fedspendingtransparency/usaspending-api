summary_file = """
    WITH published_dabs_toptier_agency AS (
        SELECT distinct ta.toptier_code,
            ta.name
        FROM
            global_temp.toptier_agency ta
        INNER JOIN global_temp.agency a on
            (a.toptier_agency_id = ta.toptier_agency_id
                AND a.toptier_flag = true)
        INNER JOIN global_temp.submission_attributes sa
        ON ta.toptier_code = sa.toptier_code
        INNER JOIN global_temp.dabs_submission_window_schedule s on
            sa.submission_window_id = s.id
        WHERE
            s.submission_reveal_date <= now()
    ),
    total_budgetary_resources AS (
        SELECT
            fiscal_year,
            fiscal_period,
            SUM(total_budgetary_resources_cpe) AS total_budgetary_resources
        FROM global_temp.gtas_sf133_balances gtas
        GROUP BY fiscal_year,
                fiscal_period
    )
    SELECT
        pdta.name AS agency_name,
        pdta.toptier_code AS toptier_code,
        rao.fiscal_year,
        rao.fiscal_period,
        ((SUM(rao.total_budgetary_resources) / MIN(tbr.total_budgetary_resources)) * 100) AS percent_of_total_federal_budget,
        MAX(sa.published_date) AS most_recent_update,
        COUNT(rmt.tas_rendering_label) AS missing_tas_accounts_count,
        SUM(rao.total_diff_approp_ocpa_obligated_amounts) AS reporting_difference_in_obligations,
        SUM(rao.unlinked_procurement_d_awards) as unlinked_contract_awards_in_contract_data,
        SUM(rao.unlinked_procurement_c_awards) as unlinked_contract_awards_in_award_spending_breakdown_data,
        SUM(rao.unlinked_assistance_d_awards) as unlinked_assistance_awards_in_financial_assistance_data,
        SUM(rao.unlinked_assistance_c_awards) as unlinked_assistance_awards_in_award_spending_breakdown_data,
        SUM(rao.unlinked_procurement_c_awards + rao.unlinked_procurement_d_awards) + SUM(rao.unlinked_assistance_c_awards + rao.unlinked_assistance_d_awards) AS total_unlinked_awards
    FROM published_dabs_toptier_agency AS pdta

    INNER JOIN global_temp.reporting_agency_overview rao
        ON rao.toptier_code = pdta.toptier_code

    LEFT JOIN total_budgetary_resources tbr
        ON rao.fiscal_year = tbr.fiscal_year
        AND rao.fiscal_period = tbr.fiscal_period

    LEFT JOIN global_temp.submission_attributes sa
        ON sa.toptier_code = pdta.toptier_code
            AND sa.reporting_fiscal_year = rao.fiscal_year
            AND sa.reporting_fiscal_period = rao.fiscal_period

    LEFT JOIN global_temp.reporting_agency_missing_tas rmt
        ON pdta.toptier_code = rmt.toptier_code
            AND rao.fiscal_year = rmt.fiscal_year
            AND rao.fiscal_period = rmt.fiscal_period
    GROUP BY
        pdta.name,
        pdta.toptier_code,
        rao.fiscal_year,
        rao.fiscal_period
"""
