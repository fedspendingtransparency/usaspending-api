-- When only URI is populated in File C, update File C assistance records to
-- link to a corresponding award if there is a single exact match based on URI.
UPDATE financial_accounts_by_awards AS faba
SET
    award_id = (
        SELECT
            award_id
        FROM
            {file_d_table} AS aw
        WHERE
            UPPER(aw.uri) = UPPER(faba.uri)
    )
WHERE
    faba.financial_accounts_by_awards_id = ANY(
        SELECT
            faba_sub.financial_accounts_by_awards_id
        FROM
            financial_accounts_by_awards AS faba_sub
        WHERE
            faba_sub.uri IS NOT NULL
            AND faba_sub.fain IS NULL
            AND faba_sub.award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM {file_d_table} AS aw_sub
                WHERE UPPER(aw_sub.uri) = UPPER(faba_sub.uri)
            ) = 1
            {submission_id_clause}
    );
