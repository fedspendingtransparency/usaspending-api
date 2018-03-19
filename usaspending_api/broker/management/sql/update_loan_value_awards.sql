-- Update all awards with their latest total value for total_loan_value
UPDATE awards AS aw
SET total_loan_value = (
        SELECT SUM(face_value_loan_guarantee)
        FROM transaction_normalized AS tx_norm
        WHERE tx_norm.award_id = aw.id
    )
WHERE category='loans';