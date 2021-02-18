WITH file_a AS (
    SELECT submission_id, COUNT(aab.*) AS count
    FROM submission_attributes sa
    LEFT OUTER JOIN appropriation_account_balances AS aab USING (submission_id)
    GROUP BY submission_id
), file_b AS (
    SELECT submission_id, COUNT(fapaoc.*) AS count
    FROM submission_attributes sa
    LEFT OUTER JOIN financial_accounts_by_program_activity_object_class AS fapaoc USING (submission_id)
    GROUP BY submission_id
), file_c AS (
    SELECT sa.submission_id, COUNT(faba.*) AS count
    FROM submission_attributes sa
    LEFT OUTER JOIN financial_accounts_by_awards faba USING (submission_id)
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
        FROM      certified_object_class_program_activity
        WHERE     LENGTH(object_class) = 4
        GROUP BY  submission_id, tas_id, program_activity_code, object_class, disaster_emergency_fund_code
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
            SELECT COUNT(*) FROM certified_appropriation AS ca
            INNER JOIN valid_tas ON valid_tas.account_num = ca.tas_id
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
                        FROM certified_object_class_program_activity AS cocpa
                        INNER JOIN valid_tas ON valid_tas.account_num = cocpa.tas_id
                        WHERE (
                            submission_id = s.submission_id
                        )
                        GROUP BY
                            cocpa.submission_id,
                            cocpa.job_id,
                            cocpa.agency_identifier,
                            cocpa.allocation_transfer_agency,
                            cocpa.availability_type_code,
                            cocpa.beginning_period_of_availa,
                            cocpa.ending_period_of_availabil,
                            cocpa.main_account_code,
                            RIGHT(cocpa.object_class, 3),
                            CASE
                                WHEN length(cocpa.object_class) = 4 AND LEFT(cocpa.object_class, 1) = ''1'' THEN ''D''
                                WHEN length(cocpa.object_class) = 4 AND LEFT(cocpa.object_class, 1) = ''2'' THEN ''R''
                                ELSE by_direct_reimbursable_fun
                            END,
                            cocpa.program_activity_code,
                            cocpa.program_activity_name,
                            cocpa.sub_account_code,
                            cocpa.tas,
                            cocpa.tas_id,
                            cocpa.disaster_emergency_fund_code
                    ) temp_file_b
                )
                ELSE
                    (
                        SELECT COUNT(*)
                        FROM certified_object_class_program_activity AS cocpa
                        INNER JOIN valid_tas ON valid_tas.account_num = cocpa.tas_id
                        WHERE (
                            submission_id = s.submission_id
                        )
                    )
                END
        AS count_file_b,
        (
            SELECT COUNT(*)
            FROM certified_award_financial AS caf
            INNER JOIN valid_tas ON valid_tas.account_num = caf.tas_id
            WHERE (
                submission_id = s.submission_id
                AND (
                    (
                        caf.transaction_obligated_amou IS NOT NULL
                        AND caf.transaction_obligated_amou != 0
                    ) OR (
                            caf.gross_outlay_amount_by_awa_cpe IS NOT NULL AND
                            caf.gross_outlay_amount_by_awa_cpe != 0
                    )
                )
            )
        ) AS count_file_c
    FROM submission s
    WHERE
            s.d2_submission = FALSE
        AND s.publish_status_id IN (2, 3)'
) AS bs (submission_id integer, count_file_a integer, count_file_b integer, count_file_c integer) USING (submission_id)
WHERE
       file_a.count IS DISTINCT FROM bs.count_file_a
    OR file_b.count IS DISTINCT FROM bs.count_file_b
    OR file_c.count IS DISTINCT FROM bs.count_file_c
;
