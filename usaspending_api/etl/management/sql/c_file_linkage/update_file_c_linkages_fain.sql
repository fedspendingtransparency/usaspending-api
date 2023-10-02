-- When only FAIN is populated in File C, update File C assistance records to
-- link to a corresponding award if there is a single exact match based on FAIN.
UPDATE financial_accounts_by_awards AS faba
SET
    award_id = (
        SELECT
            award_id
        FROM
            {file_d_table} AS aw
        WHERE
            UPPER(aw.fain) = UPPER(faba.fain)
    )
WHERE
    faba.financial_accounts_by_awards_id = ANY(
        SELECT
            faba_sub.financial_accounts_by_awards_id
        FROM (
            SELECT fain, uri, award_id, financial_accounts_by_awards_id, submission_id
            FROM financial_accounts_by_awards AS faba_sub
        ) AS faba_sub
        JOIN (
            SELECT fain
            FROM {file_d_table} as aw_sub
            GROUP BY fain
            HAVING count(*) = 1
        ) AS aw_sub
        ON UPPER(aw_sub.fain) = UPPER(faba_sub.fain)
        WHERE
            faba_sub.fain IS NOT NULL
            AND faba_sub.uri IS NULL
            AND faba_sub.award_id IS NULL
            {submission_id_clause}
    );
