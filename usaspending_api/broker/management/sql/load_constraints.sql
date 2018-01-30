-- Primary keys for dependencies
ALTER TABLE transaction_fpds ADD PRIMARY KEY (transaction_id);
ALTER TABLE transaction_fabs ADD PRIMARY KEY (transaction_id);
ALTER TABLE references_location ADD PRIMARY KEY (location_id);
ALTER TABLE legal_entity ADD PRIMARY KEY (legal_entity_id);
ALTER TABLE transaction_normalized ADD PRIMARY KEY (id);
ALTER TABLE awards ADD PRIMARY KEY (id);

-- Transaction FPDS table
ALTER TABLE transaction_fpds ADD CONSTRAINT tx_fpds_tx_norm_fk FOREIGN KEY (transaction_id) REFERENCES transaction_normalized (id);

-- Transaction FABS table
ALTER TABLE transaction_fabs ADD CONSTRAINT tx_fabs_tx_norm_fk FOREIGN KEY (transaction_id) REFERENCES transaction_normalized (id);

-- Location table

-- Legal Entity table
ALTER TABLE legal_entity ADD CONSTRAINT le_location_fk FOREIGN KEY (location_id) REFERENCES references_location (location_id);

-- Transaction Normalized table
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_ppop_location_fk FOREIGN KEY (place_of_performance_id) REFERENCES references_location (location_id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_legal_entity_fk FOREIGN KEY (recipient_id) REFERENCES legal_entity (legal_entity_id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_awarding_agency_fk FOREIGN KEY (awarding_agency_id) REFERENCES agency (id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_funding_agency_fk FOREIGN KEY (funding_agency_id) REFERENCES agency (id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_award_fk FOREIGN KEY (award_id) REFERENCES awards (id);

-- Awards table
ALTER TABLE awards ADD CONSTRAINT award_latest_tx_fk FOREIGN KEY (latest_transaction_id) REFERENCES transaction_normalized (id);
ALTER TABLE awards ADD CONSTRAINT award_ppop_location_fk FOREIGN KEY (place_of_performance_id) REFERENCES references_location (location_id);
ALTER TABLE awards ADD CONSTRAINT award_legal_entity_fk FOREIGN KEY (recipient_id) REFERENCES legal_entity (legal_entity_id);
ALTER TABLE awards ADD CONSTRAINT award_awarding_agency_fk FOREIGN KEY (awarding_agency_id) REFERENCES agency (id);
ALTER TABLE awards ADD CONSTRAINT award_funding_agency_fk FOREIGN KEY (funding_agency_id) REFERENCES agency (id);

-- Create indexes
