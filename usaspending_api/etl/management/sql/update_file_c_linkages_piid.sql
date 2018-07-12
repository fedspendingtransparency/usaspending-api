-- When PIID is populated, update File C contract records to link to a corresponding award if there is an exact
-- match based on PIID and where Parent PIID also matches using IS NOT DISTINCT FROM so it can match NULL values.
UPDATE financial_accounts_by_awards AS faba
SET
	award_id = (
		SELECT
			id
		FROM
			awards AS aw
		WHERE
			aw.piid = faba.piid
			AND
			aw.parent_award_piid IS NOT DISTINCT FROM faba.parent_award_id
	)
WHERE
	faba.financial_accounts_by_awards_id = ANY(
		SELECT
			faba_sub.financial_accounts_by_awards_id
		FROM
			financial_accounts_by_awards AS faba_sub
		WHERE
			faba_sub.piid IS NOT NULL
			AND
			faba_sub.award_id IS NULL
			AND
			(
				SELECT COUNT(*)
				FROM awards AS aw_sub
				WHERE
					aw_sub.piid = faba_sub.piid
					AND
					aw_sub.parent_award_piid IS NOT DISTINCT FROM faba_sub.parent_award_id
			) = 1
	);