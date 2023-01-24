-- If there are any records in the FABA table with an award_id that does NOT exist
--    in the temp.award_search_temp table then update the FABA.award_id column to NULL.

WITH award_id_cte AS (
	SELECT
		faba.award_id as faba_award_id
	FROM
 		financial_accounts_by_awards faba
	LEFT JOIN
		temp.award_search_temp ast
	ON
		faba.award_id = ast.award_id
	WHERE
		ast.award_id IS NULL
		AND
    	faba.award_id IS NOT NULL
)

UPDATE
	financial_accounts_by_awards
SET
	award_id = NULL
FROM
	award_id_cte
WHERE
	award_id = award_id_cte.faba_award_id;