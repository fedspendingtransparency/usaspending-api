-- If there are any records in the FABA table with an award_id that does NOT exist
--    in the temp.award_search_temp table then update the FABA.award_id column to NULL.

UPDATE
    financial_accounts_by_awards faba
SET
    faba.award_id = NULL
WHERE
    faba.award_id NOT IN (
        SELECT
            DISTINCT(award_id)
        FROM
            temp.award_search_temp
    );