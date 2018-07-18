-- Get the count of all unlinked File C records
SELECT
    COUNT(*)
FROM
    financial_accounts_by_awards
WHERE
    (fain IS NOT NULL OR uri IS NOT NULL)
    AND
    award_id IS NULL;