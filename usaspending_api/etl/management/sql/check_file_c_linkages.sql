-- Get the count of all unlinked File C records
SELECT
    COUNT(*)
FROM
    financial_accounts_by_awards
WHERE
    award_id IS NULL;