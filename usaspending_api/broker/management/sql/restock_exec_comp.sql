DROP TABLE IF EXISTS references_legalentityofficers_new;

CREATE TABLE references_legalentityofficers_new AS (
    SELECT
        legal_entity.legal_entity_id AS legal_entity_id,
        broker_exec_comp.duns AS duns,
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
        NOW()::DATE AS update_date
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
                NULLIF(high_comp_officer1_amount, '''')::NUMERIC(20, 2) AS officer_1_amount,
                NULLIF(high_comp_officer2_amount, '''')::NUMERIC(20, 2) AS officer_2_amount,
                NULLIF(high_comp_officer3_amount, '''')::NUMERIC(20, 2) AS officer_3_amount,
                NULLIF(high_comp_officer4_amount, '''')::NUMERIC(20, 2) AS officer_4_amount,
                NULLIF(high_comp_officer5_amount, '''')::NUMERIC(20, 2) AS officer_5_amount
            FROM executive_compensation e
            INNER JOIN (
                SELECT awardee_or_recipient_uniqu, max(created_at) as MaxDate
                FROM executive_compensation ex
                GROUP BY awardee_or_recipient_uniqu
            ) ex ON e.awardee_or_recipient_uniqu = ex.awardee_or_recipient_uniqu
                AND e.created_at = ex.MaxDate
            WHERE
            TRIM(high_comp_officer1_full_na) != '''' OR
            TRIM(high_comp_officer2_full_na) != '''' OR
            TRIM(high_comp_officer3_full_na) != '''' OR
            TRIM(high_comp_officer4_full_na) != '''' OR
            TRIM(high_comp_officer5_full_na) != '''' OR
            TRIM(high_comp_officer1_amount) != '''' OR
            TRIM(high_comp_officer2_amount) != '''' OR
            TRIM(high_comp_officer3_amount) != '''' OR
            TRIM(high_comp_officer4_amount) != '''' OR
            TRIM(high_comp_officer5_amount) != '''')') AS broker_exec_comp
            (
                duns text,
                officer_1_name text,
                officer_2_name text,
                officer_3_name text,
                officer_4_name text,
                officer_5_name text,
                officer_1_amount numeric(20, 2),
                officer_2_amount numeric(20, 2),
                officer_3_amount numeric(20, 2),
                officer_4_amount numeric(20, 2),
                officer_5_amount numeric(20, 2)
            )
            INNER JOIN
            legal_entity ON legal_entity.recipient_unique_id = broker_exec_comp.duns
);

BEGIN;
TRUNCATE TABLE references_legalentityofficers RESTART IDENTITY;

INSERT INTO public.references_legalentityofficers (
    legal_entity_id, duns,
    officer_1_name, officer_2_name, officer_3_name, officer_4_name, officer_5_name,
    officer_1_amount, officer_2_amount, officer_3_amount, officer_4_amount, officer_5_amount,
    update_date)
  SELECT
    legal_entity_id,
    duns,
    officer_1_name,
    officer_2_name,
    officer_3_name,
    officer_4_name,
    officer_5_name,
    officer_1_amount,
    officer_2_amount,
    officer_3_amount,
    officer_4_amount,
    officer_5_amount,
    update_date
  FROM public.references_legalentityofficers_new;

DROP TABLE references_legalentityofficers_new;


UPDATE external_data_load_date SET last_load_date=now()
WHERE external_data_type_id = (
    SELECT external_data_type_id
    FROM external_data_type
    WHERE external_data_type.name = 'exec_comp');

COMMIT;