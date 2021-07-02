DROP VIEW IF EXISTS vw_transaction_search_download;
CREATE VIEW vw_transaction_search_download AS
SELECT
    transaction_search.*,
    all_revealed_faba.treasury_accounts_funding_this_award,
    all_revealed_faba.federal_accounts_funding_this_award,
    all_revealed_faba.object_classes_funding_this_award,
    all_revealed_faba.program_activities_funding_this_award
FROM transaction_search
LEFT OUTER JOIN (
    SELECT
        faba.award_id,
        STRING_AGG(DISTINCT taa.tas_rendering_label, ';') AS treasury_accounts_funding_this_award,
        STRING_AGG(DISTINCT fa.federal_account_code, ';') AS federal_accounts_funding_this_award,
        STRING_AGG(
            DISTINCT CASE
                WHEN faba.object_class_id IS NOT NULL
                THEN CONCAT(oc.object_class, ': ', oc.object_class_name)
                ELSE NULL
            END,
            ';'
        ) AS object_classes_funding_this_award,
        STRING_AGG(
            DISTINCT CASE
                WHEN faba.program_activity_id IS NOT NULL
                THEN CONCAT(rpa.program_activity_code, ': ', rpa.program_activity_name)
                ELSE NULL
            END,
            ';'
        ) AS program_activities_funding_this_award
    FROM financial_accounts_by_awards AS faba
    LEFT OUTER JOIN treasury_appropriation_account AS taa ON (faba.treasury_account_id = taa.treasury_account_identifier)
    LEFT OUTER JOIN federal_account AS fa ON (taa.federal_account_id = fa.id)
    LEFT OUTER JOIN object_class AS oc ON (faba.object_class_id = oc.id)
    LEFT OUTER JOIN ref_program_activity AS rpa ON (faba.program_activity_id = rpa.id)
    WHERE faba.award_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM submission_attributes AS sa
        INNER JOIN dabs_submission_window_schedule AS dsws ON (sa.submission_window_id = dsws.id)
        WHERE faba.submission_id = sa.submission_id AND dsws.submission_reveal_date <= NOW()
    )
    GROUP BY faba.award_id
) AS all_revealed_faba ON (transaction_search.award_id = all_revealed_faba.award_id)
;
