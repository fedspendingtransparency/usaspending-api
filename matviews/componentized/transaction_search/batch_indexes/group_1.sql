CREATE INDEX idx_21c979c6$49d_action_date_temp ON transaction_search_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_type_temp ON transaction_search_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_recipient_unique_id_temp ON transaction_search_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_recipient_hash_temp ON transaction_search_temp USING BTREE(recipient_hash) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
