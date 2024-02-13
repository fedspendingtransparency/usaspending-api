file_c_sql_string = """
    WITH valid_file_c AS (
    SELECT
        distinct
        ta.toptier_code,
        faba.piid,
        faba.fain,
        faba.uri,
        faba.award_id,
        faba.distinct_award_key,
        CASE
            WHEN faba.piid IS NOT NULL THEN true
            ELSE false
        END AS is_fpds,
        sa.reporting_fiscal_year AS fiscal_year,
        sa.reporting_fiscal_quarter AS fiscal_quarter,
        sa.reporting_fiscal_period AS fiscal_period,
        sa.quarter_format_flag
    FROM
        int.financial_accounts_by_awards AS faba
    INNER JOIN
    global_temp.submission_attributes AS sa
    ON
        faba.submission_id = sa.submission_id
    INNER JOIN
    global_temp.dabs_submission_window_schedule AS dsws ON
        (
    sa.submission_window_id = dsws.id
            and dsws.submission_reveal_date <= NOW()
    )
    INNER JOIN
    global_temp.treasury_appropriation_account AS taa ON
        (taa.treasury_account_identifier = faba.treasury_account_id)
    INNER JOIN
    global_temp.toptier_agency AS ta ON
        (taa.funding_toptier_agency_id = ta.toptier_agency_id)
    WHERE
        faba.transaction_obligated_amount IS NOT NULL
        and sa.reporting_fiscal_year >= 2017
    )
    SELECT
        toptier_code,
        piid,
        fain,
        uri,
        award_id,
        is_fpds,
        fiscal_year,
        fiscal_period
    FROM
        valid_file_c
    WHERE
        award_id is null
"""
