DROP TABLE IF EXISTS references_legalentityofficers_new;

CREATE TABLE references_legalentityofficers_new AS (
    SELECT
        legal_entity.legal_entity_id as legal_entity_id,
        broker_exec_comp.officer_1_name AS officer_1_name,
        broker_exec_comp.officer_2_name AS officer_2_name,
        broker_exec_comp.officer_3_name AS officer_3_name,
        broker_exec_comp.officer_4_name AS officer_4_name,
        broker_exec_comp.officer_5_name AS officer_5_name,
        broker_exec_comp.officer_1_amount AS officer_1_amount,
        broker_exec_comp.officer_2_amount AS officer_2_amount,
        broker_exec_comp.officer_3_amount AS officer_3_amount,
        broker_exec_comp.officer_4_amount AS officer_4_amount,
        broker_exec_comp.officer_5_amount AS officer_5_amount,
        NOW() AS update_date
    FROM
        dblink ('broker_server', '(
            SELECT
                DISTINCT ON (e.awardee_or_recipient_uniqu)
                e.awardee_or_recipient_uniqu AS duns,
                high_comp_officer1_full_na AS officer_1_name,
                high_comp_officer2_full_na AS officer_2_name,
                high_comp_officer3_full_na AS officer_3_name,
                high_comp_officer4_full_na AS officer_4_name,
                high_comp_officer5_full_na AS officer_5_name,
                high_comp_officer1_amount AS officer_1_amount,
                high_comp_officer2_amount AS officer_2_amount,
                high_comp_officer3_amount AS officer_3_amount,
                high_comp_officer4_amount AS officer_4_amount,
                high_comp_officer5_amount AS officer_5_amount
            FROM executive_compensation e
            INNER JOIN (
                SELECT awardee_or_recipient_uniqu, max(created_at) as MaxDate
                FROM executive_compensation ex
                GROUP BY awardee_or_recipient_uniqu
            ) ex ON e.awardee_or_recipient_uniqu = ex.awardee_or_recipient_uniqu
                AND e.created_at = ex.MaxDate
            WHERE
            TRIM(high_comp_officer1_full_na) != '' OR
            TRIM(high_comp_officer2_full_na) != '' OR
            TRIM(high_comp_officer3_full_na) != '' OR
            TRIM(high_comp_officer4_full_na) != '' OR
            TRIM(high_comp_officer5_full_na) != '' OR
            TRIM(high_comp_officer1_amount) != '' OR
            TRIM(high_comp_officer2_amount) != '' OR
            TRIM(high_comp_officer3_amount) != '' OR
            TRIM(high_comp_officer4_amount) != '' OR
            TRIM(high_comp_officer5_amount) != '')') AS broker_exec_comp
            (
            	duns text,
                officer_1_name text,
                officer_2_name text,
                officer_3_name text,
                officer_4_name text,
                officer_5_name text,
                officer_1_amount text,
                officer_2_amount text,
                officer_3_amount text,
                officer_4_amount text,
                officer_5_amount text
            )
            INNER JOIN
            legal_entity ON legal_entity.recipient_unique_id = broker_exec_comp.duns
);