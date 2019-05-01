-- Delete all legal_entity records that are no longer referenced by
-- transaction, award, or subaward data

BEGIN;

DELETE
FROM references_legalentityofficers
WHERE legal_entity_id IN (
    SELECT le.legal_entity_id
    FROM legal_entity le
    WHERE (
        NOT EXISTS (SELECT 1 FROM transaction_normalized WHERE recipient_id = le.legal_entity_id) AND
        NOT EXISTS (SELECT 1 FROM awards WHERE recipient_id = le.legal_entity_id) AND
        NOT EXISTS (SELECT 1 FROM subaward WHERE prime_recipient_id = le.legal_entity_id)
    )
);

DELETE
FROM legal_entity le
WHERE (
    NOT EXISTS (SELECT 1 FROM transaction_normalized WHERE recipient_id = le.legal_entity_id) AND
    NOT EXISTS (SELECT 1 FROM awards WHERE recipient_id = le.legal_entity_id) AND
    NOT EXISTS (SELECT 1 FROM subaward WHERE prime_recipient_id = le.legal_entity_id)
);

COMMIT;

