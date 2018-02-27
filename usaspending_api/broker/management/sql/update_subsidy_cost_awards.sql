-- Update all awards with their latest total value for total_subsidy_cost
UPDATE awards AS aw
SET total_subsidy_cost = (
        SELECT SUM(original_loan_subsidy_cost)
        FROM transaction_normalized AS tx_norm
        WHERE tx_norm.award_id = aw.id
    )
WHERE category='loans';