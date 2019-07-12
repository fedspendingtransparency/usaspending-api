-- Delete all legal_entity records that are no longer referenced by
-- transaction, award, or subaward data

BEGIN;

DELETE
FROM legal_entity le
WHERE (
    NOT EXISTS (SELECT 1 FROM transaction_normalized WHERE recipient_id = le.legal_entity_id) AND
    NOT EXISTS (SELECT 1 FROM awards WHERE recipient_id = le.legal_entity_id)
);

COMMIT;

