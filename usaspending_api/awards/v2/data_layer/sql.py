defc_sql = """
    SELECT
        DISTINCT disaster_emergency_fund_code,
        COALESCE(sum(CASE WHEN latest_closed_period_per_fy.is_quarter IS NOT NULL THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS total_outlay,
        COALESCE(sum(faba.transaction_obligated_amount), 0) AS obligated_amount
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
    LEFT JOIN (
        SELECT   submission_fiscal_year, is_quarter, max(submission_fiscal_month) AS submission_fiscal_month
        FROM     dabs_submission_window_schedule
        WHERE    submission_reveal_date < now() AND period_start_date >= '2020-04-01'
        GROUP BY submission_fiscal_year, is_quarter
    ) AS latest_closed_period_per_fy
        ON latest_closed_period_per_fy.submission_fiscal_year = sa.reporting_fiscal_year
        AND latest_closed_period_per_fy.submission_fiscal_month = sa.reporting_fiscal_period
        AND latest_closed_period_per_fy.is_quarter = sa.quarter_format_flag
    WHERE {award_id_sql}
    GROUP BY disaster_emergency_fund_code
    ORDER BY obligated_amount desc
    """
