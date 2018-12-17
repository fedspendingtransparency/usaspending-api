-- Update all awards with their base_exercised_options_val, fpds_agency_id, and fpds_parent_agency_id
WITH txn_totals AS (
    SELECT
        tx.award_id,
        f.agency_id,
        f.referenced_idv_agency_iden,
        SUM(CAST(f.base_exercised_options_val AS double precision)) AS base_exercised_options_val
    FROM transaction_fpds AS f
    INNER JOIN transaction_normalized AS tx ON f.transaction_id = tx.id
    GROUP BY tx.award_id, f.agency_id, f.referenced_idv_agency_iden
)
UPDATE awards a
SET
    base_exercised_options_val = t.base_exercised_options_val,
    fpds_agency_id = t.agency_id,
    fpds_parent_agency_id = t.referenced_idv_agency_iden
FROM txn_totals AS t
WHERE t.award_id = a.id
