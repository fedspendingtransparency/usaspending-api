ALTER TABLE transaction_fabs RENAME TO transaction_fabs_old;
ALTER TABLE transaction_fpds RENAME TO transaction_fpds_old;
ALTER TABLE references_location RENAME TO references_location_old;
ALTER TABLE legal_entity RENAME TO legal_entity_old;
ALTER TABLE transaction_normalized RENAME TO transaction_normalized_old;
ALTER TABLE awards RENAME TO awards_old;

ALTER TABLE transaction_fabs_new RENAME TO transaction_fabs;
ALTER TABLE transaction_fpds_new RENAME TO transaction_fpds;
ALTER TABLE references_location_new RENAME TO references_location;
ALTER TABLE legal_entity_new RENAME TO legal_entity;
ALTER TABLE transaction_normalized_new RENAME TO transaction_normalized;
ALTER TABLE awards_new RENAME TO awards;

TRUNCATE awards_old, transaction_normalized_old, legal_entity_old, references_location_old, transaction_fabs_old, transaction_fpds_old CASCADE;

DROP TABLE awards_old CASCADE;
DROP TABLE transaction_normalized_old CASCADE;
DROP TABLE legal_entity_old CASCADE;
DROP TABLE transaction_fabs_old CASCADE;
DROP TABLE transaction_fpds_old CASCADE;
DROP TABLE references_location_old CASCADE;