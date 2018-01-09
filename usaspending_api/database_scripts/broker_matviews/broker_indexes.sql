-- FPDS Indexes
CREATE INDEX dap_sort_group_2 ON detached_award_procurement USING btree (piid, parent_award_id, agency_id, referenced_idv_agency_iden, action_date DESC, award_modification_amendme DESC, transaction_number DESC);
CREATE INDEX dap_sort_group_1 ON detached_award_procurement USING btree (piid, parent_award_id, agency_id, referenced_idv_agency_iden);
CREATE INDEX dap_agency_id_idx ON detached_award_procurement USING btree (agency_id) WHERE (parent_award_id IS NOT NULL);
CREATE INDEX dap_referenced_idv_agency_iden_idx ON detached_award_procurement USING btree (referenced_idv_agency_iden);
CREATE INDEX dap_uniq_3_cols_idx ON detached_award_procurement USING btree (piid, parent_award_id, awarding_sub_tier_agency_c);
CREATE INDEX dap_parent_award_id_idx ON detached_award_procurement USING btree (parent_award_id) WHERE (parent_award_id IS NOT NULL);
CREATE INDEX dap_pd_of_perf_end_desc_idx ON detached_award_procurement USING btree (period_of_performance_curr DESC);
CREATE INDEX dap_uniq_2_cols_idx ON detached_award_procurement USING btree (piid, awarding_sub_tier_agency_c);
CREATE INDEX dap_piid_idx ON detached_award_procurement USING btree (piid);
CREATE INDEX dap_pd_of_perf_start_asc_idx ON detached_award_procurement USING btree (period_of_performance_star);
CREATE INDEX dap_awarding_subtier_code_idx ON detached_award_procurement USING btree (awarding_sub_tier_agency_c);
CREATE INDEX dap_base_and_all_options_idx ON detached_award_procurement USING btree (((base_and_all_options_value)::double precision));
CREATE INDEX dap_action_date_desc_idx ON detached_award_procurement USING btree (action_date DESC);
CREATE INDEX dap_fed_action_obligation_idx ON detached_award_procurement USING btree (((federal_action_obligation)::double precision));
CREATE INDEX dap_group_1 on detached_award_procurement USING btree (piid, parent_award_id, agency_id, referenced_idv_agency_iden, action_date, award_modification_amendme, transaction_number);
CREATE INDEX dap_group_2 on detached_award_procurement USING btree (piid, parent_award_id, agency_id, referenced_idv_agency_iden) ;

-- FABS Indexes
CREATE INDEX pafa_record_type_idx ON published_award_financial_assistance USING btree (record_type);
CREATE INDEX pafa_fed_action_obligation_idx ON published_award_financial_assistance USING btree (((federal_action_obligation)::double precision));
CREATE INDEX pafa_pd_of_perf_end_desc_idx ON published_award_financial_assistance USING btree (period_of_performance_curr DESC);
CREATE INDEX pafa_pd_of_perf_start_asc_idx ON published_award_financial_assistance USING btree (period_of_performance_star);
CREATE INDEX pafa_action_date_desc_idx ON published_award_financial_assistance USING btree (action_date DESC);
CREATE INDEX pafa_awarding_subtier_code_idx ON published_award_financial_assistance USING btree (awarding_sub_tier_agency_c);
CREATE INDEX pafa_uri_idx ON published_award_financial_assistance USING btree (uri);
CREATE INDEX pafa_fain_idx ON published_award_financial_assistance USING btree (fain);
CREATE INDEX pafa_uniq_3_cols_idx ON published_award_financial_assistance USING btree (fain, uri, awarding_sub_tier_agency_c);
CREATE INDEX pafa_afa_generated_unique_idx ON published_award_financial_assistance USING btree (afa_generated_unique DESC);
