-- Get the count of all unlinked File C assistance records
SELECT
    COUNT(*)
FROM
    financial_accounts_by_awards
WHERE
    piid IS NULL  -- if piid is null, then fain or uri is populated per GSDM
    AND award_id IS NULL
;
