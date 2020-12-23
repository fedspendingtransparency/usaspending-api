-- Update File C records with a transaction_obligated_amount of `NaN.00` to `0`
UPDATE
    financial_accounts_by_awards
SET
    transaction_obligated_amount = 0
WHERE
    transaction_obligated_amount = 'nan'
;
