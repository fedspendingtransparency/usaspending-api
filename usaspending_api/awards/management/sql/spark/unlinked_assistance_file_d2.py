file_d2_sql_string = """
    WITH valid_file_c AS (
    SELECT
        distinct
    ta.toptier_code,
        faba.award_id,
        faba.distinct_award_key,
        CASE
            when faba.piid is NOT null THEN true
            else false
        end AS is_fpds,
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
            AND dsws.submission_reveal_date <= now()
    )
    INNER JOIN
    global_temp.treasury_appropriation_account AS taa ON
        (taa.treasury_account_identifier = faba.treasury_account_id)
    INNER JOIN
    global_temp.toptier_agency AS ta ON
        (taa.funding_toptier_agency_id = ta.toptier_agency_id)
    WHERE
        faba.transaction_obligated_amount is NOT null
        AND sa.reporting_fiscal_year >= 2017
    ),
    quarterly_flag AS (
    SELECT
        distinct
    toptier_code,
        fiscal_year,
        fiscal_quarter,
        quarter_format_flag
    FROM
        valid_file_c
    WHERE
        quarter_format_flag = true
    ),
    valid_file_d AS (
    SELECT
        fa.toptier_code,
        a.award_id AS award_id,
        ts.detached_award_proc_unique AS assistance_transaction_unique_key,
        ts.generated_unique_award_id AS assistance_award_unique_key,
        ts.piid AS award_id_piid,
        ts.modification_number AS modification_number,
        ts.transaction_number AS transaction_number,
        a.is_fpds,
        ts.fiscal_year,
        CASE
            when ql.quarter_format_flag = true THEN ts.fiscal_quarter * 3
            when ts.fiscal_period = 1 THEN 2
            else ts.fiscal_period
        end AS fiscal_period
    FROM
        rpt.award_search AS a
    INNER JOIN
    (
        SELECT
            *,
            date_part('quarter', tn.action_date + interval '3' month) AS fiscal_quarter,
            date_part('month', tn.action_date + interval '3' month) AS fiscal_period
        FROM
            rpt.transaction_search AS tn
        WHERE
            tn.action_date >= '2016-10-01'
            AND tn.awarding_agency_id is NOT NULL
    ) AS ts ON
        (ts.award_id = a.award_id)
    INNER JOIN lateral (
        SELECT
            ta.toptier_code
        FROM
            global_temp.agency AS ag
        INNER JOIN global_temp.toptier_agency AS ta ON
            (ag.toptier_agency_id = ta.toptier_agency_id)
        WHERE
            ag.id = a.awarding_agency_id
    ) AS fa ON
        1=1
    LEFT JOIN
    quarterly_flag AS ql ON
        (
    fa.toptier_code = ql.toptier_code
            AND ts.fiscal_year = ql.fiscal_year
            AND ts.fiscal_quarter = ql.fiscal_quarter
    )
    WHERE
        (
    (a.type IN ('07', '08')
            AND a.total_subsidy_cost > 0)
        OR a.type NOT IN ('07', '08')
    )
            AND a.certified_date >= '2016-10-01'
        group by
            fa.toptier_code,
            a.award_id,
            ts.detached_award_proc_unique,
            ts.generated_unique_award_id,
            ts.piid,
            ts.modification_number,
            ts.transaction_number,
            a.is_fpds,
            ts.fiscal_year,
            CASE
                WHEN ql.quarter_format_flag = true THEN ts.fiscal_quarter * 3
                WHEN ts.fiscal_period = 1 THEN 2
                ELSE ts.fiscal_period
            END
    )
    SELECT
        toptier_code,
        award_id,
        assistance_transaction_unique_key,
        assistance_award_unique_key,
        award_id_piid,
        modification_number,
        transaction_number,
        fiscal_year,
        fiscal_period
    FROM
        valid_file_d AS vfd
    WHERE
        NOT exists (
        SELECT
            1
        FROM
            int.financial_accounts_by_awards AS faba
        WHERE
            faba.award_id = vfd.award_id
    )
        AND is_fpds = false
"""
