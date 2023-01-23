-- Count the rows in the FABA table where the award_id column is not NULL and is not in the temp.award_search_temp table

SELECT
    COUNT(*)
FROM
    financial_accounts_by_awards faba
WHERE
    faba.award_id IS NOT NULL
    AND
    faba.award_id NOT IN (
        SELECT
            DISTINCT(award_id)
        FROM
            temp.award_search_temp
    );
