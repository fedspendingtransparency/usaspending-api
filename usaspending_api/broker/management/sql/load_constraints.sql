-- Primary keys for dependencies
ALTER TABLE transaction_fpds ADD PRIMARY KEY (transaction_id);
ALTER TABLE transaction_fabs ADD PRIMARY KEY (transaction_id);
ALTER TABLE references_location ADD PRIMARY KEY (location_id);
ALTER TABLE legal_entity ADD PRIMARY KEY (legal_entity_id);
ALTER TABLE transaction_normalized ADD PRIMARY KEY (id);
ALTER TABLE awards ADD PRIMARY KEY (id);
ALTER TABLE references_legalentityofficers ADD PRIMARY KEY(legal_entity_id);

-- Transaction FPDS table
ALTER TABLE transaction_fpds ADD CONSTRAINT tx_fpds_tx_norm_fk FOREIGN KEY (transaction_id) REFERENCES transaction_normalized (id) DEFERRABLE INITIALLY DEFERRED;

-- Transaction FABS table
ALTER TABLE transaction_fabs ADD CONSTRAINT tx_fabs_tx_norm_fk FOREIGN KEY (transaction_id) REFERENCES transaction_normalized (id) DEFERRABLE INITIALLY DEFERRED;

-- Location table
ALTER TABLE references_location ALTER location_id SET DEFAULT NEXTVAL('references_location_id_seq');

-- Legal Entity table
ALTER TABLE legal_entity ALTER legal_entity_id SET DEFAULT NEXTVAL('legal_entity_id_seq');
ALTER TABLE legal_entity ADD CONSTRAINT le_location_fk FOREIGN KEY (location_id) REFERENCES references_location (location_id) DEFERRABLE INITIALLY DEFERRED;

-- Transaction Normalized table
ALTER TABLE transaction_normalized ALTER id SET DEFAULT NEXTVAL('tx_norm_id_seq');
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_ppop_location_fk FOREIGN KEY (place_of_performance_id) REFERENCES references_location (location_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_legal_entity_fk FOREIGN KEY (recipient_id) REFERENCES legal_entity (legal_entity_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_awarding_agency_fk FOREIGN KEY (awarding_agency_id) REFERENCES agency (id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_funding_agency_fk FOREIGN KEY (funding_agency_id) REFERENCES agency (id);
ALTER TABLE transaction_normalized ADD CONSTRAINT tx_norm_award_fk FOREIGN KEY (award_id) REFERENCES awards (id) DEFERRABLE INITIALLY DEFERRED;

-- Awards table
ALTER TABLE awards ALTER id SET DEFAULT NEXTVAL('award_id_seq');
ALTER TABLE awards ADD CONSTRAINT award_latest_tx_fk FOREIGN KEY (latest_transaction_id) REFERENCES transaction_normalized (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE awards ADD CONSTRAINT award_ppop_location_fk FOREIGN KEY (place_of_performance_id) REFERENCES references_location (location_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE awards ADD CONSTRAINT award_legal_entity_fk FOREIGN KEY (recipient_id) REFERENCES legal_entity (legal_entity_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE awards ADD CONSTRAINT award_awarding_agency_fk FOREIGN KEY (awarding_agency_id) REFERENCES agency (id);
ALTER TABLE awards ADD CONSTRAINT award_funding_agency_fk FOREIGN KEY (funding_agency_id) REFERENCES agency (id);

-- Subawards table
ALTER TABLE awards_subaward ADD CONSTRAINT awards_subaward_award_fk FOREIGN KEY (award_id) REFERENCES awards (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE awards_subaward ADD CONSTRAINT awards_subaward_legal_entity_fk FOREIGN KEY (recipient_id) REFERENCES legal_entity (legal_entity_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE awards_subaward ADD CONSTRAINT awards_subaward_ppop_location_fk FOREIGN KEY (place_of_performance_id) REFERENCES references_location (location_id) DEFERRABLE INITIALLY DEFERRED;

-- Financial accounts by award table
ALTER TABLE financial_accounts_by_awards ADD CONSTRAINT faba_award_fk FOREIGN KEY (award_id) REFERENCES awards (id) DEFERRABLE INITIALLY DEFERRED;

-- Create indexes

-- Locations
CREATE INDEX references_location_county_name_adce16cf ON references_location USING btree (county_name);
CREATE INDEX references_location_county_name_adce16cf_like ON references_location USING btree (county_name text_pattern_ops);
CREATE INDEX references_location_county_code_84c81bba ON references_location USING btree (county_code);
CREATE INDEX references_location_county_code_84c81bba_like ON references_location USING btree (county_code text_pattern_ops);

-- Legal Entity
CREATE INDEX legal_entity_business_types_0be08b40 ON legal_entity USING btree (business_types);
CREATE INDEX legal_entity_business_types_0be08b40_like ON legal_entity USING btree (business_types text_pattern_ops);
CREATE INDEX legal_entity_recipient_unique_id_4e48f7fb ON legal_entity USING btree (recipient_unique_id);
CREATE INDEX legal_entity_recipient_unique_id_4e48f7fb_like ON legal_entity USING btree (recipient_unique_id text_pattern_ops);
CREATE INDEX legal_entity_domestic_or_foreign_entity_2404797e ON legal_entity USING btree (domestic_or_foreign_entity);
CREATE INDEX legal_entity_domestic_or_foreign_entity_2404797e_like ON legal_entity USING btree (domestic_or_foreign_entity text_pattern_ops);
CREATE INDEX legal_entity_location_id_7f712296 ON legal_entity USING btree (location_id);

-- Transaction FABS
CREATE UNIQUE INDEX transaction_fabs_afa_generated_unique_key ON transaction_fabs USING btree (afa_generated_unique);
CREATE INDEX transaction_fabs_afa_generated_unique_bb7b8f4b_like ON transaction_fabs USING btree (afa_generated_unique text_pattern_ops);
CREATE UNIQUE INDEX transaction_fabs_awarding_sub_tier_agency_cc5ccd22_uniq ON transaction_fabs USING btree (awarding_sub_tier_agency_c, award_modification_amendme, fain, uri);
CREATE INDEX transaction_fabs_cfda_number_0222a383 ON transaction_fabs USING btree (cfda_number);
CREATE INDEX transaction_fabs_cfda_number_0222a383_like ON transaction_fabs USING btree (cfda_number text_pattern_ops);
CREATE INDEX transaction_fabs_fain_c98ae4fd ON transaction_fabs USING btree (fain);
CREATE INDEX transaction_fabs_fain_c98ae4fd_like ON transaction_fabs USING btree (fain text_pattern_ops);
CREATE INDEX transaction_fabs_published_award_financial__7a0c4b63 ON transaction_fabs USING btree (published_award_financial_assistance_id);
CREATE INDEX transaction_fabs_uri_b36dd710 ON transaction_fabs USING btree (uri);
CREATE INDEX transaction_fabs_uri_b36dd710_like ON transaction_fabs USING btree (uri text_pattern_ops);

-- Transaction FPDS
CREATE UNIQUE INDEX transaction_fpds_detached_award_proc_unique_key ON transaction_fpds USING btree (detached_award_proc_unique);
CREATE INDEX transaction_fpds_detached_award_proc_unique_bb7b8f4b_like ON transaction_fpds USING btree (detached_award_proc_unique text_pattern_ops);
CREATE INDEX transaction_fpds_extent_competed_a21d5514 ON transaction_fpds USING btree (extent_competed);
CREATE INDEX transaction_fpds_extent_competed_a21d5514_like ON transaction_fpds USING btree (extent_competed text_pattern_ops);
CREATE INDEX transaction_fpds_naics_cb8d93d1 ON transaction_fpds USING btree (naics);
CREATE INDEX transaction_fpds_naics_cb8d93d1_like ON transaction_fpds USING btree (naics text_pattern_ops);
CREATE INDEX transaction_fpds_product_or_service_code_1aca7d16 ON transaction_fpds USING btree (product_or_service_code);
CREATE INDEX transaction_fpds_product_or_service_code_1aca7d16_like ON transaction_fpds USING btree (product_or_service_code text_pattern_ops);
CREATE INDEX transaction_fpds_type_of_contract_pricing_d0d08c92 ON transaction_fpds USING btree (type_of_contract_pricing);
CREATE INDEX transaction_fpds_type_of_contract_pricing_d0d08c92_like ON transaction_fpds USING btree (type_of_contract_pricing text_pattern_ops);
CREATE INDEX transaction_fpds_type_set_aside_23fece56 ON transaction_fpds USING btree (type_set_aside);
CREATE INDEX transaction_fpds_type_set_aside_23fece56_like ON transaction_fpds USING btree (type_set_aside text_pattern_ops);
CREATE INDEX transaction_fpds_detached_award_procurement_id_eeddae57 ON transaction_fpds USING btree (detached_award_procurement_id);
CREATE INDEX transaction_fpds_piid_77c637c0 ON transaction_fpds USING btree (piid);
CREATE INDEX transaction_fpds_piid_77c637c0_like ON transaction_fpds USING btree (piid text_pattern_ops);
CREATE INDEX transaction_fpds_parent_award_id_fa12cf5a ON transaction_fpds USING btree (parent_award_id);
CREATE INDEX transaction_fpds_parent_award_id_fa12cf5a_like ON transaction_fpds USING btree (parent_award_id text_pattern_ops);

-- Transaction Normalized
CREATE INDEX transaction_normalized_award_id_action_date_a1a6bf4c_idx ON transaction_normalized USING btree (award_id, action_date);
CREATE INDEX transaction_normalized_type_bea849f6 ON transaction_normalized USING btree (type);
CREATE INDEX transaction_normalized_type_bea849f6_like ON transaction_normalized USING btree (type text_pattern_ops);
CREATE INDEX transaction_normalized_action_date_a9b524c1 ON transaction_normalized USING btree (action_date);
CREATE INDEX transaction_normalized_federal_action_obligation_ffd51f1b ON transaction_normalized USING btree (federal_action_obligation);
CREATE INDEX transaction_normalized_award_id_aab95fbf ON transaction_normalized USING btree (award_id);
CREATE INDEX transaction_normalized_awarding_agency_id_27b1cd91 ON transaction_normalized USING btree (awarding_agency_id);
CREATE INDEX transaction_normalized_funding_agency_id_27ab4331 ON transaction_normalized USING btree (funding_agency_id);
CREATE INDEX transaction_normalized_place_of_performance_id_ab44d845 ON transaction_normalized USING btree (place_of_performance_id);
CREATE INDEX transaction_normalized_recipient_id_f8f784f2 ON transaction_normalized USING btree (recipient_id);
CREATE INDEX transaction_normalized_update_date_850daa8b ON transaction_normalized USING btree (update_date);

-- Awards
CREATE INDEX awards_type_4c2f4316 ON awards USING btree (type);
CREATE INDEX awards_type_4c2f4316_like ON awards USING btree (type text_pattern_ops);
CREATE INDEX awards_piid_5bb316be ON awards USING btree (piid);
CREATE INDEX awards_piid_5bb316be_like ON awards USING btree (piid text_pattern_ops);
CREATE INDEX awards_fain_ed8c63f1 ON awards USING btree (fain);
CREATE INDEX awards_fain_ed8c63f1_like ON awards USING btree (fain text_pattern_ops);
CREATE INDEX awards_uri_6982773c ON awards USING btree (uri);
CREATE INDEX awards_uri_6982773c_like ON awards USING btree (uri text_pattern_ops);
CREATE INDEX awards_total_obligation_7cdeba76 ON awards USING btree (total_obligation);
CREATE INDEX awards_total_outlay_9e745534 ON awards USING btree (total_outlay);
CREATE INDEX awards_date_signed_edd8cc5b ON awards USING btree (date_signed);
--CREATE INDEX awards_description_926c9c54 ON awards USING btree (description);
--CREATE INDEX awards_description_926c9c54_like ON awards USING btree (description text_pattern_ops);
CREATE INDEX awards_period_of_performance_start_date_2c08e64a ON awards USING btree (period_of_performance_start_date);
CREATE INDEX awards_period_of_performance_current_end_date_fb888b8a ON awards USING btree (period_of_performance_current_end_date);
CREATE INDEX awards_potential_total_value_of_award_49a2173b ON awards USING btree (potential_total_value_of_award);
CREATE INDEX awards_base_and_all_options_value_1bee496c ON awards USING btree (base_and_all_options_value);
CREATE INDEX awards_awarding_agency_id_a8cf5df8 ON awards USING btree (awarding_agency_id);
CREATE INDEX awards_funding_agency_id_aafee1a4 ON awards USING btree (funding_agency_id);
CREATE INDEX awards_latest_transaction_id_a78dcb0f ON awards USING btree (latest_transaction_id);
CREATE INDEX awards_parent_award_id_bcf46af7 ON awards USING btree (parent_award_id);
CREATE INDEX awards_place_of_performance_id_7d7369e6 ON awards USING btree (place_of_performance_id);
CREATE INDEX awards_recipient_id_3cdf8905 ON awards USING btree (recipient_id);
CREATE INDEX awards_category_c8863e0a ON awards USING btree (category);
CREATE INDEX awards_category_c8863e0a_like ON awards USING btree (category text_pattern_ops);
CREATE INDEX awards_piid_uppr_idx ON awards USING btree (upper(piid));
CREATE INDEX awards_fain_uppr_idx ON awards USING btree (upper(fain));
CREATE INDEX awards_uri_uppr_idx ON awards USING btree (upper(uri));