-- Unlinks FABA records from Awards that no longer exist

WITH award_id_cte AS (
	SELECT
		faba.award_id as faba_award_id,
  		faba.financial_accounts_by_awards_id as faba_id
	FROM
 		financial_accounts_by_awards faba
	LEFT JOIN
		{file_d_table} a
	ON
		faba.award_id = a.award_id
	WHERE
		a.award_id IS NULL
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
	financial_accounts_by_awards_id = award_id_cte.faba_id;