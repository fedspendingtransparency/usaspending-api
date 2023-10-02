-- When both FAIN and URI are populated in File C, update File C assistance
-- records to link to a corresponding award if there is an single exact match
-- based on FAIN
UPDATE financial_accounts_by_awards AS faba
SET
    award_id = (
        SELECT
            id
        FROM
            vw_awards AS aw
        WHERE
            UPPER(aw.fain) = UPPER(faba.fain)
    )
WHERE
    faba.financial_accounts_by_awards_id = ANY(
        SELECT
            faba_sub.financial_accounts_by_awards_id
        FROM (
            SELECT fain, uri, award_id, submission_id, financial_accounts_by_awards_id
            FROM financial_accounts_by_awards AS faba_sub
        ) AS faba_sub
        JOIN (
            SELECT fain
            FROM vw_awards as aw_sub
            GROUP BY fain
            HAVING count(*) = 1
        ) AS aw_sub
        ON UPPER(aw_sub.fain) = UPPER(faba_sub.fain)
        WHERE
            faba_sub.fain IS NOT NULL
            AND faba_sub.uri IS NOT NULL
            AND faba_sub.award_id IS NULL
            {submission_id_clause}
    );


-- When both FAIN and URI are populated in File C, update File C assistance
-- records to link to a corresponding award if there is an single exact match
-- based on URI
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
        FROM (
            SELECT fain, uri, award_id, submission_id, financial_accounts_by_awards_id
            FROM financial_accounts_by_awards AS faba_sub
        ) AS faba_sub
        JOIN (
            SELECT uri
            FROM {file_d_table} AS aw_sub
            GROUP BY uri
            HAVING count(*) = 1
        ) AS aw_sub
        ON UPPER(aw_sub.uri) = UPPER(faba_sub.uri)
        WHERE
            faba_sub.fain IS NOT NULL
            AND faba_sub.uri IS NOT NULL
            AND faba_sub.award_id IS NULL
            {submission_id_clause}
        );
