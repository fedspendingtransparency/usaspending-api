-- Update FABS records since they're the only ones that could use face_value_loan_guarantee
UPDATE transaction_normalized AS tx_norm
SET face_value_loan_guarantee = tx_fabs.face_value_loan_guarantee
FROM transaction_fabs AS tx_fabs
WHERE tx_norm.id = tx_fabs.transaction_id AND tx_norm.type IN ('07', '08');