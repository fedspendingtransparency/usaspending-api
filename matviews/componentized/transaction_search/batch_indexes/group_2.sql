CREATE INDEX idx_21c979c6$49d_last_modified_date_temp ON transaction_search_temp USING BTREE(last_modified_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_21c979c6$49d_award_id_temp ON transaction_search_temp USING BTREE(award_id) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_parent_recipient_unique_id_temp ON transaction_search_temp USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97) WHERE parent_recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_21c979c6$49d_action_date_pre2008_temp ON transaction_search_temp USING BTREE(action_date) WITH (fillfactor = 97) WHERE action_date < '2007-10-01';
