-- When only FAIN is populated in File C, update File C assistance records to
-- link to a corresponding award if there is a single exact match based on FAIN.
WITH update_cte AS (
    UPDATE financial_accounts_by_awards AS faba
    SET
        award_id = (
            SELECT
                id
            FROM
                awards AS aw
            WHERE
                UPPER(aw.fain) = UPPER(faba.fain)
        )
    WHERE
        faba.financial_accounts_by_awards_id = ANY(
            SELECT
                faba_sub.financial_accounts_by_awards_id
            FROM
                financial_accounts_by_awards AS faba_sub
            WHERE
                faba_sub.fain IS NOT NULL
                AND faba_sub.uri IS NULL
                AND faba_sub.award_id IS NULL
                AND (
                    SELECT COUNT(*)
                    FROM awards AS aw_sub
                    WHERE UPPER(aw_sub.fain) = UPPER(faba_sub.fain)
                ) = 1
                {submission_id_clause}
        )
    RETURNING award_id
)
UPDATE
    awards
SET
    update_date = NOW()
FROM
    update_cte
WHERE
    id = update_cte.award_id
;
