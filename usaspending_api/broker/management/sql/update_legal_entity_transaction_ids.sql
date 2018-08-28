-- Fix the legal entity table metadata (transaction_unique_id) to its corresponding transaction
-- Faulty metadata arose through nightly loads

UPDATE legal_entity le
SET transaction_unique_id = tn.transaction_unique_id
FROM transaction_normalized AS tn
WHERE tn.recipient_id = le.legal_entity_id and UPPER(le.transaction_unique_id)='NONE';
