defc_sql = """
    SELECT
        DISTINCT disaster_emergency_fund_code,
        COALESCE(sum(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN (COALESCE(faba.gross_outlay_amount_by_award_cpe,0)
            + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
            + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)) END), 0) AS total_outlay,
        COALESCE(sum(faba.transaction_obligated_amount), 0) AS obligated_amount
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
    INNER JOIN submission_attributes sa
        ON faba.submission_id = sa.submission_id
    WHERE {award_id_sql}
    GROUP BY disaster_emergency_fund_code
    ORDER BY obligated_amount desc
    """
