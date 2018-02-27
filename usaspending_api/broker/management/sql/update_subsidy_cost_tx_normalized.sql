-- Update FABS records since they're the only ones that could use original_loan_subsidy_cost
UPDATE transaction_normalized AS tx_norm
SET original_loan_subsidy_cost = tx_fabs.original_loan_subsidy_cost
FROM transaction_fabs AS tx_fabs
WHERE tx_norm.id = tx_fabs.transaction_id AND tx_norm.type IN ('07', '08');