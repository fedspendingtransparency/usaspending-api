-- Get the count of all unlinked File C contract records
SELECT
    COUNT(*)
FROM
    financial_accounts_by_awards
WHERE
    piid IS NOT NULL
    AND award_id IS NULL
;
