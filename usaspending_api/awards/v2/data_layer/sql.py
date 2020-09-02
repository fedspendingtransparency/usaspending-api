defc_sql = """
    SELECT
        DISTINCT disaster_emergency_fund_code,
        COALESCE(sum(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS total_outlay,
        COALESCE(sum(faba.transaction_obligated_amount), 0) AS obligated_amount
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
    WHERE {award_id_sql}
    GROUP BY disaster_emergency_fund_code
    ORDER BY obligated_amount desc
    """
