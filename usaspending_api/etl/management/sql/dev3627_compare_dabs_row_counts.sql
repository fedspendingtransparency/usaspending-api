WITH file_a AS (
    SELECT submission_id, COUNT(aab.*) AS count
    FROM submission_attributes AS sa
    LEFT OUTER JOIN appropriation_account_balances AS aab USING (submission_id)
    GROUP BY submission_id
), file_b AS (
    SELECT submission_id, COUNT(fapaoc.*) AS count
    FROM submission_attributes AS sa
    LEFT OUTER JOIN financial_accounts_by_program_activity_object_class AS fapaoc USING (submission_id)
    GROUP BY submission_id
), file_c AS (
    SELECT sa.submission_id, COUNT(faba.*) AS count
    FROM submission_attributes AS sa
    LEFT OUTER JOIN financial_accounts_by_awards AS faba USING (submission_id)
    GROUP BY submission_id
)

SELECT submission_id,
    file_a.count AS usas_file_a,
    bs.count_file_a AS broker_file_a,
    file_b.count AS usas_file_b,
    bs.count_file_b AS broker_file_b,
    file_c.count AS usas_file_c,
    bs.count_file_c AS broker_file_c,
    (bs.count_file_a - file_a.count) + (bs.count_file_b - file_b.count) + (bs.count_file_c - file_c.count) AS discrepency_count
FROM
    submission_attributes sa
INNER JOIN file_a USING(submission_id)
INNER JOIN file_b USING(submission_id)
INNER JOIN file_c USING(submission_id)
INNER JOIN dblink(
    'broker_server',
    'WITH subs_with_duplicate_file_b AS (
        SELECT    DISTINCT submission_id
        FROM      published_object_class_program_activity
        WHERE     LENGTH(object_class) = 4
        GROUP BY  submission_id, account_num, program_activity_code, object_class, disaster_emergency_fund_code
        HAVING    COUNT(*) > 1
    ), valid_tas AS (
        SELECT account_num
        FROM tas_lookup
        WHERE
            financial_indicator2 IS DISTINCT FROM ''F''
    )
    SELECT
        submission_id,
        (
            SELECT COUNT(*) FROM published_appropriation AS pa
            INNER JOIN valid_tas ON valid_tas.account_num = pa.account_num
            WHERE (
                submission_id = s.submission_id
            )
        ) AS count_file_a,
        CASE
            WHEN s.submission_id IN (SELECT submission_id FROM subs_with_duplicate_file_b)
            THEN
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM published_object_class_program_activity AS pocpa
                        INNER JOIN valid_tas ON valid_tas.account_num = pocpa.account_num
                        WHERE (
                            submission_id = s.submission_id
                        )
                        GROUP BY
                            pocpa.submission_id,
                            pocpa.job_id,
                            pocpa.agency_identifier,
                            pocpa.allocation_transfer_agency,
                            pocpa.availability_type_code,
                            pocpa.beginning_period_of_availa,
                            pocpa.ending_period_of_availabil,
                            pocpa.main_account_code,
                            RIGHT(pocpa.object_class, 3),
                            CASE
                                WHEN length(pocpa.object_class) = 4 AND LEFT(pocpa.object_class, 1) = ''1'' THEN ''D''
                                WHEN length(pocpa.object_class) = 4 AND LEFT(pocpa.object_class, 1) = ''2'' THEN ''R''
                                ELSE by_direct_reimbursable_fun
                            END,
                            pocpa.program_activity_code,
                            pocpa.program_activity_name,
                            pocpa.sub_account_code,
                            pocpa.tas,
                            pocpa.account_num,
                            pocpa.disaster_emergency_fund_code
                    ) temp_file_b
                )
                ELSE
                    (
                        SELECT COUNT(*)
                        FROM published_object_class_program_activity AS pocpa
                        INNER JOIN valid_tas ON valid_tas.account_num = pocpa.account_num
                        WHERE (
                            submission_id = s.submission_id
                        )
                    )
                END
        AS count_file_b,
        (
            SELECT COUNT(*)
            FROM published_award_financial AS paf
            INNER JOIN valid_tas ON valid_tas.account_num = paf.account_num
            WHERE (
                submission_id = s.submission_id
                AND (
                    COALESCE(paf.transaction_obligated_amou, 0) != 0
                    OR COALESCE(paf.gross_outlay_amount_by_awa_cpe, 0) != 0
                    OR COALESCE(paf.ussgl487200_downward_adjus_cpe, 0) != 0
                    OR COALESCE(paf.ussgl497200_downward_adjus_cpe, 0) != 0
                )
            )
        ) AS count_file_c
    FROM submission AS s
    WHERE
            s.is_fabs = FALSE
        AND s.publish_status_id IN (2, 3)'
) AS bs (submission_id integer, count_file_a integer, count_file_b integer, count_file_c integer) USING (submission_id)
WHERE
       file_a.count IS DISTINCT FROM bs.count_file_a
    OR file_b.count IS DISTINCT FROM bs.count_file_b
    OR file_c.count IS DISTINCT FROM bs.count_file_c
;
