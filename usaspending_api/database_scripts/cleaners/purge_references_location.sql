-- Delete all references_location records that are no longer referenced by
-- transaction, award, or subaward data

BEGIN;

DELETE
FROM references_location rl
WHERE (
    NOT EXISTS (SELECT 1 FROM transaction_normalized WHERE place_of_performance_id = rl.location_id) AND
    NOT EXISTS (SELECT 1 FROM awards WHERE place_of_performance_id = rl.location_id) AND
    NOT EXISTS (SELECT 1 FROM legal_entity WHERE location_id = rl.location_id)
);

COMMIT;

