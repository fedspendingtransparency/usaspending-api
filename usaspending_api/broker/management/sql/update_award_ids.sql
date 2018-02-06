DROP TABLE IF EXISTS transaction_normalized_new_old;
DROP TABLE IF EXISTS transaction_normalized_new_2;

CREATE TABLE transaction_normalized_new_2 AS (
    SELECT
        awards_new.id AS award_id,
        transaction_normalized_new.id as id,
        transaction_normalized_new.is_fpds AS is_fpds,
        transaction_normalized_new.transaction_unique_id AS transaction_unique_id,
        transaction_normalized_new.generated_unique_award_id AS generated_unique_award_id,
        transaction_normalized_new.usaspending_unique_transaction_id AS usaspending_unique_transaction_id,
        transaction_normalized_new.type AS type,
        transaction_normalized_new.type_description AS type_description,
        transaction_normalized_new.period_of_performance_start_date AS period_of_performance_start_date,
        transaction_normalized_new.period_of_performance_current_end_date AS period_of_performance_current_end_date,
        transaction_normalized_new.action_date AS action_date,
        transaction_normalized_new.action_type AS action_type,
        transaction_normalized_new.action_type_description AS action_type_description,
        transaction_normalized_new.federal_action_obligation AS federal_action_obligation,
        transaction_normalized_new.modification_number AS modification_number,
        transaction_normalized_new.description AS description,
        transaction_normalized_new.drv_award_transaction_usaspend AS drv_award_transaction_usaspend,
        transaction_normalized_new.drv_current_total_award_value_amount_adjustment AS drv_current_total_award_value_amount_adjustment,
        transaction_normalized_new.drv_potential_total_award_value_amount_adjustment AS drv_potential_total_award_value_amount_adjustment,
        transaction_normalized_new.last_modified_date AS last_modified_date,
        transaction_normalized_new.certified_date AS certified_date,
        transaction_normalized_new.create_date AS create_date,
        transaction_normalized_new.update_date AS update_date,
        transaction_normalized_new.fiscal_year AS fiscal_year,
        transaction_normalized_new.awarding_agency_id AS awarding_agency_id,
        transaction_normalized_new.funding_agency_id AS funding_agency_id,
        transaction_normalized_new.place_of_performance_id AS place_of_performance_id,
        transaction_normalized_new.recipient_id AS recipient_id
    FROM
        transaction_normalized_new
        INNER JOIN
        awards_new ON awards_new.generated_unique_award_id = transaction_normalized_new.generated_unique_award_id
);

ALTER TABLE transaction_normalized_new RENAME TO transaction_normalized_new_old;
ALTER TABLE transaction_normalized_new_2 RENAME TO transaction_normalized_new;
TRUNCATE transaction_normalized_new_old;
DROP TABLE transaction_normalized_new_old;
