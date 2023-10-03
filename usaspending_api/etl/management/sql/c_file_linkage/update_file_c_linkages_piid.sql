-- When PIID and Parent PIID are populated, update File C contract records to
--  link to a corresponding award if there is a single exact match based on PIID
--  and Parent PIID
UPDATE financial_accounts_by_awards AS faba
SET
    award_id = (
        SELECT
            award_id
        FROM
            {file_d_table} AS aw
        WHERE
            UPPER(aw.piid) = UPPER(faba.piid)
            AND UPPER(aw.parent_award_piid) = UPPER(faba.parent_award_id)
    )
WHERE
    faba.financial_accounts_by_awards_id = ANY(
        SELECT
            faba_sub.financial_accounts_by_awards_id
        FROM (
                SELECT piid, award_id, parent_award_id, submission_id, financial_accounts_by_awards_id
                FROM financial_accounts_by_awards AS faba_sub
		) as faba_sub
		JOIN (
				SELECT piid, parent_award_piid
				FROM {file_d_table} as aw_sub
				GROUP BY piid, parent_award_piid
				HAVING count(*) = 1
		) as aw_sub
		ON UPPER(aw_sub.piid) = UPPER(faba_sub.piid)
        AND UPPER(aw_sub.parent_award_piid) = UPPER(faba_sub.parent_award_id)
        WHERE
            faba_sub.piid IS NOT NULL
            AND faba_sub.award_id IS NULL
            AND faba_sub.parent_award_id IS NOT NULL
            {submission_id_clause}
    );


-- When PIID is populated and Parent PIID is NULL, update File C contract
--  records to link to a corresponding award if there is a single exact match
-- based on PIID
UPDATE financial_accounts_by_awards AS faba
SET
    award_id = (
        SELECT
            award_id
        FROM
            {file_d_table} AS aw
        WHERE
            UPPER(aw.piid) = UPPER(faba.piid)
    )
WHERE
    faba.financial_accounts_by_awards_id = ANY(
        SELECT
            faba_sub.financial_accounts_by_awards_id
        FROM (
            SELECT piid, award_id, parent_award_id, submission_id, financial_accounts_by_awards_id
            FROM financial_accounts_by_awards AS faba_sub
        ) as faba_sub
        JOIN (
            SELECT piid
            FROM {file_d_table} as aw_sub
            GROUP BY piid
            HAVING count(*) = 1
        ) as aw_sub
            ON UPPER(aw_sub.piid) = UPPER(faba_sub.piid)
        WHERE
            faba_sub.piid IS NOT NULL
            AND faba_sub.award_id IS NULL
            AND faba_sub.parent_award_id IS NULL
            {submission_id_clause}
    );
