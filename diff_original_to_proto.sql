SELECT
    old.transaction_id,
    jsonb_strip_nulls(
        jsonb_build_object(
            'transaction_id', CASE WHEN old."transaction_id" IS DISTINCT FROM new."transaction_id" THEN jsonb_build_object('old', old."transaction_id", 'new', new."transaction_id") ELSE null END,
            'detached_award_proc_unique', CASE WHEN old."detached_award_proc_unique" IS DISTINCT FROM new."detached_award_proc_unique" THEN jsonb_build_object('old', old."detached_award_proc_unique", 'new', new."detached_award_proc_unique") ELSE null END,
            'afa_generated_unique', CASE WHEN old."afa_generated_unique" IS DISTINCT FROM new."afa_generated_unique" THEN jsonb_build_object('old', old."afa_generated_unique", 'new', new."afa_generated_unique") ELSE null END,
            'update_date', CASE WHEN old."update_date" IS DISTINCT FROM new."etl_update_date" THEN jsonb_build_object('old', old."update_date", 'new', new."etl_update_date") ELSE null END,
            'modification_number', CASE WHEN old."modification_number" IS DISTINCT FROM new."modification_number" THEN jsonb_build_object('old', old."modification_number", 'new', new."modification_number") ELSE null END,
            'generated_unique_award_id', CASE WHEN old."generated_unique_award_id" IS DISTINCT FROM new."generated_unique_award_id" THEN jsonb_build_object('old', old."generated_unique_award_id", 'new', new."generated_unique_award_id") ELSE null END,
            'award_id', CASE WHEN old."award_id" IS DISTINCT FROM new."award_id" THEN jsonb_build_object('old', old."award_id", 'new', new."award_id") ELSE null END,
            'piid', CASE WHEN old."piid" IS DISTINCT FROM new."piid" THEN jsonb_build_object('old', old."piid", 'new', new."piid") ELSE null END,
            'fain', CASE WHEN old."fain" IS DISTINCT FROM new."fain" THEN jsonb_build_object('old', old."fain", 'new', new."fain") ELSE null END,
            'uri', CASE WHEN old."uri" IS DISTINCT FROM new."uri" THEN jsonb_build_object('old', old."uri", 'new', new."uri") ELSE null END,
            'award_description', CASE WHEN old."award_description" IS DISTINCT FROM new."transaction_description" THEN jsonb_build_object('old', old."award_description", 'new', new."transaction_description") ELSE null END,
            'product_or_service_code', CASE WHEN old."product_or_service_code" IS DISTINCT FROM new."product_or_service_code" THEN jsonb_build_object('old', old."product_or_service_code", 'new', new."product_or_service_code") ELSE null END,
            'product_or_service_description', CASE WHEN old."product_or_service_description" IS DISTINCT FROM new."product_or_service_description" THEN jsonb_build_object('old', old."product_or_service_description", 'new', new."product_or_service_description") ELSE null END,
            'psc_agg_key', CASE WHEN old."psc_agg_key" IS DISTINCT FROM new."psc_agg_key" THEN jsonb_build_object('old', old."psc_agg_key", 'new', new."psc_agg_key") ELSE null END,
            'naics_code', CASE WHEN old."naics_code" IS DISTINCT FROM new."naics_code" THEN jsonb_build_object('old', old."naics_code", 'new', new."naics_code") ELSE null END,
            'naics_description', CASE WHEN old."naics_description" IS DISTINCT FROM new."naics_description" THEN jsonb_build_object('old', old."naics_description", 'new', new."naics_description") ELSE null END,
            'naics_agg_key', CASE WHEN old."naics_agg_key" IS DISTINCT FROM new."naics_agg_key" THEN jsonb_build_object('old', old."naics_agg_key", 'new', new."naics_agg_key") ELSE null END,
            'type_description', CASE WHEN old."type_description" IS DISTINCT FROM new."type_description" THEN jsonb_build_object('old', old."type_description", 'new', new."type_description") ELSE null END,
            'award_category', CASE WHEN old."award_category" IS DISTINCT FROM new."award_category" THEN jsonb_build_object('old', old."award_category", 'new', new."award_category") ELSE null END,
            'recipient_unique_id', CASE WHEN old."recipient_unique_id" IS DISTINCT FROM new."recipient_unique_id" THEN jsonb_build_object('old', old."recipient_unique_id", 'new', new."recipient_unique_id") ELSE null END,
            'recipient_name', CASE WHEN old."recipient_name" IS DISTINCT FROM new."recipient_name" THEN jsonb_build_object('old', old."recipient_name", 'new', new."recipient_name") ELSE null END,
            'recipient_hash', CASE WHEN old."recipient_hash" IS DISTINCT FROM new."recipient_hash" THEN jsonb_build_object('old', old."recipient_hash", 'new', new."recipient_hash") ELSE null END,
            'recipient_agg_key', CASE WHEN old."recipient_agg_key" IS DISTINCT FROM new."recipient_agg_key" THEN jsonb_build_object('old', old."recipient_agg_key", 'new', new."recipient_agg_key") ELSE null END,
            'parent_recipient_unique_id', CASE WHEN old."parent_recipient_unique_id" IS DISTINCT FROM new."parent_recipient_unique_id" THEN jsonb_build_object('old', old."parent_recipient_unique_id", 'new', new."parent_recipient_unique_id") ELSE null END,
            'parent_recipient_name', CASE WHEN old."parent_recipient_name" IS DISTINCT FROM new."parent_recipient_name" THEN jsonb_build_object('old', old."parent_recipient_name", 'new', new."parent_recipient_name") ELSE null END
        ) || jsonb_build_object(
            'parent_recipient_hash', CASE WHEN old."parent_recipient_hash" IS DISTINCT FROM new."parent_recipient_hash" THEN jsonb_build_object('old', old."parent_recipient_hash", 'new', new."parent_recipient_hash") ELSE null END,
            'action_date', CASE WHEN old."action_date" IS DISTINCT FROM new."action_date" THEN jsonb_build_object('old', old."action_date", 'new', new."action_date") ELSE null END,
            'fiscal_action_date', CASE WHEN old."fiscal_action_date" IS DISTINCT FROM new."fiscal_action_date" THEN jsonb_build_object('old', old."fiscal_action_date", 'new', new."fiscal_action_date") ELSE null END,
            'period_of_performance_start_date', CASE WHEN old."period_of_performance_start_date" IS DISTINCT FROM new."period_of_performance_start_date" THEN jsonb_build_object('old', old."period_of_performance_start_date", 'new', new."period_of_performance_start_date") ELSE null END,
            'period_of_performance_current_end_date', CASE WHEN old."period_of_performance_current_end_date" IS DISTINCT FROM new."period_of_performance_current_end_date" THEN jsonb_build_object('old', old."period_of_performance_current_end_date", 'new', new."period_of_performance_current_end_date") ELSE null END,
            'ordering_period_end_date', CASE WHEN old."ordering_period_end_date" IS DISTINCT FROM new."ordering_period_end_date" THEN jsonb_build_object('old', old."ordering_period_end_date", 'new', new."ordering_period_end_date") ELSE null END,
            'transaction_fiscal_year', CASE WHEN old."transaction_fiscal_year" IS DISTINCT FROM new."fiscal_year" THEN jsonb_build_object('old', old."transaction_fiscal_year", 'new', new."fiscal_year") ELSE null END,
            'award_fiscal_year', CASE WHEN old."award_fiscal_year" IS DISTINCT FROM new."award_fiscal_year" THEN jsonb_build_object('old', old."award_fiscal_year", 'new', new."award_fiscal_year") ELSE null END,
            'award_amount', CASE WHEN old."award_amount" IS DISTINCT FROM new."award_amount" THEN jsonb_build_object('old', old."award_amount", 'new', new."award_amount") ELSE null END,
            'transaction_amount', CASE WHEN old."transaction_amount" IS DISTINCT FROM new."federal_action_obligation" THEN jsonb_build_object('old', old."transaction_amount", 'new', new."federal_action_obligation") ELSE null END,
            'face_value_loan_guarantee', CASE WHEN old."face_value_loan_guarantee" IS DISTINCT FROM new."face_value_loan_guarantee" THEN jsonb_build_object('old', old."face_value_loan_guarantee", 'new', new."face_value_loan_guarantee") ELSE null END,
            'original_loan_subsidy_cost', CASE WHEN old."original_loan_subsidy_cost" IS DISTINCT FROM new."original_loan_subsidy_cost" THEN jsonb_build_object('old', old."original_loan_subsidy_cost", 'new', new."original_loan_subsidy_cost") ELSE null END,
            'generated_pragmatic_obligation', CASE WHEN old."generated_pragmatic_obligation" IS DISTINCT FROM new."generated_pragmatic_obligation" THEN jsonb_build_object('old', old."generated_pragmatic_obligation", 'new', new."generated_pragmatic_obligation") ELSE null END,
            'awarding_agency_id', CASE WHEN old."awarding_agency_id" IS DISTINCT FROM new."awarding_agency_id" THEN jsonb_build_object('old', old."awarding_agency_id", 'new', new."awarding_agency_id") ELSE null END,
            'funding_agency_id', CASE WHEN old."funding_agency_id" IS DISTINCT FROM new."funding_agency_id" THEN jsonb_build_object('old', old."funding_agency_id", 'new', new."funding_agency_id") ELSE null END,
            'awarding_toptier_agency_name', CASE WHEN old."awarding_toptier_agency_name" IS DISTINCT FROM new."awarding_toptier_agency_name" THEN jsonb_build_object('old', old."awarding_toptier_agency_name", 'new', new."awarding_toptier_agency_name") ELSE null END,
            'funding_toptier_agency_name', CASE WHEN old."funding_toptier_agency_name" IS DISTINCT FROM new."funding_toptier_agency_name" THEN jsonb_build_object('old', old."funding_toptier_agency_name", 'new', new."funding_toptier_agency_name") ELSE null END,
            'awarding_subtier_agency_name', CASE WHEN old."awarding_subtier_agency_name" IS DISTINCT FROM new."awarding_subtier_agency_name" THEN jsonb_build_object('old', old."awarding_subtier_agency_name", 'new', new."awarding_subtier_agency_name") ELSE null END,
            'funding_subtier_agency_name', CASE WHEN old."funding_subtier_agency_name" IS DISTINCT FROM new."funding_subtier_agency_name" THEN jsonb_build_object('old', old."funding_subtier_agency_name", 'new', new."funding_subtier_agency_name") ELSE null END,
            'awarding_toptier_agency_abbreviation', CASE WHEN old."awarding_toptier_agency_abbreviation" IS DISTINCT FROM new."awarding_toptier_agency_abbreviation" THEN jsonb_build_object('old', old."awarding_toptier_agency_abbreviation", 'new', new."awarding_toptier_agency_abbreviation") ELSE null END,
            'funding_toptier_agency_abbreviation', CASE WHEN old."funding_toptier_agency_abbreviation" IS DISTINCT FROM new."funding_toptier_agency_abbreviation" THEN jsonb_build_object('old', old."funding_toptier_agency_abbreviation", 'new', new."funding_toptier_agency_abbreviation") ELSE null END,
            'awarding_subtier_agency_abbreviation', CASE WHEN old."awarding_subtier_agency_abbreviation" IS DISTINCT FROM new."awarding_subtier_agency_abbreviation" THEN jsonb_build_object('old', old."awarding_subtier_agency_abbreviation", 'new', new."awarding_subtier_agency_abbreviation") ELSE null END,
            'funding_subtier_agency_abbreviation', CASE WHEN old."funding_subtier_agency_abbreviation" IS DISTINCT FROM new."funding_subtier_agency_abbreviation" THEN jsonb_build_object('old', old."funding_subtier_agency_abbreviation", 'new', new."funding_subtier_agency_abbreviation") ELSE null END,
            'awarding_toptier_agency_agg_key', CASE WHEN old."awarding_toptier_agency_agg_key" IS DISTINCT FROM new."awarding_toptier_agency_agg_key" THEN jsonb_build_object('old', old."awarding_toptier_agency_agg_key", 'new', new."awarding_toptier_agency_agg_key") ELSE null END,
            'funding_toptier_agency_agg_key', CASE WHEN old."funding_toptier_agency_agg_key" IS DISTINCT FROM new."funding_toptier_agency_agg_key" THEN jsonb_build_object('old', old."funding_toptier_agency_agg_key", 'new', new."funding_toptier_agency_agg_key") ELSE null END
        ) || jsonb_build_object(
            'awarding_subtier_agency_agg_key', CASE WHEN old."awarding_subtier_agency_agg_key" IS DISTINCT FROM new."awarding_subtier_agency_agg_key" THEN jsonb_build_object('old', old."awarding_subtier_agency_agg_key", 'new', new."awarding_subtier_agency_agg_key") ELSE null END,
            'funding_subtier_agency_agg_key', CASE WHEN old."funding_subtier_agency_agg_key" IS DISTINCT FROM new."funding_subtier_agency_agg_key" THEN jsonb_build_object('old', old."funding_subtier_agency_agg_key", 'new', new."funding_subtier_agency_agg_key") ELSE null END,
            'cfda_number', CASE WHEN old."cfda_number" IS DISTINCT FROM new."cfda_number" THEN jsonb_build_object('old', old."cfda_number", 'new', new."cfda_number") ELSE null END,
            'cfda_title', CASE WHEN old."cfda_title" IS DISTINCT FROM new."cfda_title" THEN jsonb_build_object('old', old."cfda_title", 'new', new."cfda_title") ELSE null END,
            'type_of_contract_pricing', CASE WHEN old."type_of_contract_pricing" IS DISTINCT FROM new."type_of_contract_pricing" THEN jsonb_build_object('old', old."type_of_contract_pricing", 'new', new."type_of_contract_pricing") ELSE null END,
            'type_set_aside', CASE WHEN old."type_set_aside" IS DISTINCT FROM new."type_set_aside" THEN jsonb_build_object('old', old."type_set_aside", 'new', new."type_set_aside") ELSE null END,
            'extent_competed', CASE WHEN old."extent_competed" IS DISTINCT FROM new."extent_competed" THEN jsonb_build_object('old', old."extent_competed", 'new', new."extent_competed") ELSE null END,
            'type', CASE WHEN old."type" IS DISTINCT FROM new."type" THEN jsonb_build_object('old', old."type", 'new', new."type") ELSE null END,
            'pop_country_code', CASE WHEN old."pop_country_code" IS DISTINCT FROM new."pop_country_code" THEN jsonb_build_object('old', old."pop_country_code", 'new', new."pop_country_code") ELSE null END,
            'pop_country_name', CASE WHEN old."pop_country_name" IS DISTINCT FROM new."pop_country_name" THEN jsonb_build_object('old', old."pop_country_name", 'new', new."pop_country_name") ELSE null END,
            'pop_state_code', CASE WHEN old."pop_state_code" IS DISTINCT FROM new."pop_state_code" THEN jsonb_build_object('old', old."pop_state_code", 'new', new."pop_state_code") ELSE null END,
            'pop_county_code', CASE WHEN old."pop_county_code" IS DISTINCT FROM new."pop_county_code" THEN jsonb_build_object('old', old."pop_county_code", 'new', new."pop_county_code") ELSE null END,
            'pop_county_name', CASE WHEN old."pop_county_name" IS DISTINCT FROM new."pop_county_name" THEN jsonb_build_object('old', old."pop_county_name", 'new', new."pop_county_name") ELSE null END,
            'pop_zip5', CASE WHEN old."pop_zip5" IS DISTINCT FROM new."pop_zip5" THEN jsonb_build_object('old', old."pop_zip5", 'new', new."pop_zip5") ELSE null END,
            'pop_congressional_code', CASE WHEN old."pop_congressional_code" IS DISTINCT FROM new."pop_congressional_code" THEN jsonb_build_object('old', old."pop_congressional_code", 'new', new."pop_congressional_code") ELSE null END,
            'pop_city_name', CASE WHEN old."pop_city_name" IS DISTINCT FROM new."pop_city_name" THEN jsonb_build_object('old', old."pop_city_name", 'new', new."pop_city_name") ELSE null END,
            'pop_county_agg_key', CASE WHEN old."pop_county_agg_key" IS DISTINCT FROM new."pop_county_agg_key" THEN jsonb_build_object('old', old."pop_county_agg_key", 'new', new."pop_county_agg_key") ELSE null END,
            'pop_congressional_agg_key', CASE WHEN old."pop_congressional_agg_key" IS DISTINCT FROM new."pop_congressional_agg_key" THEN jsonb_build_object('old', old."pop_congressional_agg_key", 'new', new."pop_congressional_agg_key") ELSE null END,
            'pop_state_agg_key', CASE WHEN old."pop_state_agg_key" IS DISTINCT FROM new."pop_state_agg_key" THEN jsonb_build_object('old', old."pop_state_agg_key", 'new', new."pop_state_agg_key") ELSE null END,
            'pop_country_agg_key', CASE WHEN old."pop_country_agg_key" IS DISTINCT FROM new."pop_country_agg_key" THEN jsonb_build_object('old', old."pop_country_agg_key", 'new', new."pop_country_agg_key") ELSE null END,
            'recipient_location_country_code', CASE WHEN old."recipient_location_country_code" IS DISTINCT FROM new."recipient_location_country_code" THEN jsonb_build_object('old', old."recipient_location_country_code", 'new', new."recipient_location_country_code") ELSE null END,
            'recipient_location_country_name', CASE WHEN old."recipient_location_country_name" IS DISTINCT FROM new."recipient_location_country_name" THEN jsonb_build_object('old', old."recipient_location_country_name", 'new', new."recipient_location_country_name") ELSE null END,
            'recipient_location_state_code', CASE WHEN old."recipient_location_state_code" IS DISTINCT FROM new."recipient_location_state_code" THEN jsonb_build_object('old', old."recipient_location_state_code", 'new', new."recipient_location_state_code") ELSE null END,
            'recipient_location_county_code', CASE WHEN old."recipient_location_county_code" IS DISTINCT FROM new."recipient_location_county_code" THEN jsonb_build_object('old', old."recipient_location_county_code", 'new', new."recipient_location_county_code") ELSE null END
        ) || jsonb_build_object(
            'recipient_location_county_name', CASE WHEN old."recipient_location_county_name" IS DISTINCT FROM new."recipient_location_county_name" THEN jsonb_build_object('old', old."recipient_location_county_name", 'new', new."recipient_location_county_name") ELSE null END,
            'recipient_location_zip5', CASE WHEN old."recipient_location_zip5" IS DISTINCT FROM new."recipient_location_zip5" THEN jsonb_build_object('old', old."recipient_location_zip5", 'new', new."recipient_location_zip5") ELSE null END,
            'recipient_location_congressional_code', CASE WHEN old."recipient_location_congressional_code" IS DISTINCT FROM new."recipient_location_congressional_code" THEN jsonb_build_object('old', old."recipient_location_congressional_code", 'new', new."recipient_location_congressional_code") ELSE null END,
            'recipient_location_city_name', CASE WHEN old."recipient_location_city_name" IS DISTINCT FROM new."recipient_location_city_name" THEN jsonb_build_object('old', old."recipient_location_city_name", 'new', new."recipient_location_city_name") ELSE null END,
            'recipient_location_county_agg_key', CASE WHEN old."recipient_location_county_agg_key" IS DISTINCT FROM new."recipient_location_county_agg_key" THEN jsonb_build_object('old', old."recipient_location_county_agg_key", 'new', new."recipient_location_county_agg_key") ELSE null END,
            'recipient_location_congressional_agg_key', CASE WHEN old."recipient_location_congressional_agg_key" IS DISTINCT FROM new."recipient_location_congressional_agg_key" THEN jsonb_build_object('old', old."recipient_location_congressional_agg_key", 'new', new."recipient_location_congressional_agg_key") ELSE null END,
            'recipient_location_state_agg_key', CASE WHEN old."recipient_location_state_agg_key" IS DISTINCT FROM new."recipient_location_state_agg_key" THEN jsonb_build_object('old', old."recipient_location_state_agg_key", 'new', new."recipient_location_state_agg_key") ELSE null END,
            'tas_paths', CASE WHEN old."tas_paths" IS DISTINCT FROM new."tas_paths" THEN jsonb_build_object('old', old."tas_paths", 'new', new."tas_paths") ELSE null END,
            'tas_components', CASE WHEN old."tas_components" IS DISTINCT FROM new."tas_components" THEN jsonb_build_object('old', old."tas_components", 'new', new."tas_components") ELSE null END,
            'federal_accounts', CASE WHEN old."federal_accounts" IS DISTINCT FROM new."federal_accounts" THEN jsonb_build_object('old', old."federal_accounts", 'new', new."federal_accounts") ELSE null END,
            'business_categories', CASE WHEN old."business_categories" IS DISTINCT FROM new."business_categories" THEN jsonb_build_object('old', old."business_categories", 'new', new."business_categories") ELSE null END,
            'disaster_emergency_fund_codes', CASE WHEN old."disaster_emergency_fund_codes" IS DISTINCT FROM new."disaster_emergency_fund_codes" THEN jsonb_build_object('old', old."disaster_emergency_fund_codes", 'new', new."disaster_emergency_fund_codes") ELSE null END
        )
    ) AS json_diff

FROM transaction_delta_tableview AS old
LEFT JOIN universal_transaction_matview_proto AS new USING (transaction_id)
WHERE
old."transaction_id" IS DISTINCT FROM new."transaction_id"
OR old."detached_award_proc_unique" IS DISTINCT FROM new."detached_award_proc_unique"
OR old."afa_generated_unique" IS DISTINCT FROM new."afa_generated_unique"
OR old."update_date" IS DISTINCT FROM new."etl_update_date"
OR old."modification_number" IS DISTINCT FROM new."modification_number"
OR old."generated_unique_award_id" IS DISTINCT FROM new."generated_unique_award_id"
OR old."award_id" IS DISTINCT FROM new."award_id"
OR old."piid" IS DISTINCT FROM new."piid"
OR old."fain" IS DISTINCT FROM new."fain"
OR old."uri" IS DISTINCT FROM new."uri"
OR old."award_description" IS DISTINCT FROM new."transaction_description"
OR old."product_or_service_code" IS DISTINCT FROM new."product_or_service_code"
OR old."product_or_service_description" IS DISTINCT FROM new."product_or_service_description"
OR old."psc_agg_key" IS DISTINCT FROM new."psc_agg_key"
OR old."naics_code" IS DISTINCT FROM new."naics_code"
OR old."naics_description" IS DISTINCT FROM new."naics_description"
OR old."naics_agg_key" IS DISTINCT FROM new."naics_agg_key"
OR old."type_description" IS DISTINCT FROM new."type_description"
OR old."award_category" IS DISTINCT FROM new."award_category"
OR old."recipient_unique_id" IS DISTINCT FROM new."recipient_unique_id"
OR old."recipient_name" IS DISTINCT FROM new."recipient_name"
OR old."recipient_hash" IS DISTINCT FROM new."recipient_hash"
OR old."recipient_agg_key" IS DISTINCT FROM new."recipient_agg_key"
OR old."parent_recipient_unique_id" IS DISTINCT FROM new."parent_recipient_unique_id"
OR old."parent_recipient_name" IS DISTINCT FROM new."parent_recipient_name"
OR old."parent_recipient_hash" IS DISTINCT FROM new."parent_recipient_hash"
OR old."action_date" IS DISTINCT FROM new."action_date"
OR old."fiscal_action_date" IS DISTINCT FROM new."fiscal_action_date"
OR old."period_of_performance_start_date" IS DISTINCT FROM new."period_of_performance_start_date"
OR old."period_of_performance_current_end_date" IS DISTINCT FROM new."period_of_performance_current_end_date"
OR old."ordering_period_end_date" IS DISTINCT FROM new."ordering_period_end_date"
OR old."transaction_fiscal_year" IS DISTINCT FROM new."fiscal_year"
OR old."award_fiscal_year" IS DISTINCT FROM new."award_fiscal_year"
OR old."award_amount" IS DISTINCT FROM new."award_amount"
OR old."transaction_amount" IS DISTINCT FROM new."federal_action_obligation"
OR old."face_value_loan_guarantee" IS DISTINCT FROM new."face_value_loan_guarantee"
OR old."original_loan_subsidy_cost" IS DISTINCT FROM new."original_loan_subsidy_cost"
OR old."generated_pragmatic_obligation" IS DISTINCT FROM new."generated_pragmatic_obligation"
OR old."awarding_agency_id" IS DISTINCT FROM new."awarding_agency_id"
OR old."funding_agency_id" IS DISTINCT FROM new."funding_agency_id"
OR old."awarding_toptier_agency_name" IS DISTINCT FROM new."awarding_toptier_agency_name"
OR old."funding_toptier_agency_name" IS DISTINCT FROM new."funding_toptier_agency_name"
OR old."awarding_subtier_agency_name" IS DISTINCT FROM new."awarding_subtier_agency_name"
OR old."funding_subtier_agency_name" IS DISTINCT FROM new."funding_subtier_agency_name"
OR old."awarding_toptier_agency_abbreviation" IS DISTINCT FROM new."awarding_toptier_agency_abbreviation"
OR old."funding_toptier_agency_abbreviation" IS DISTINCT FROM new."funding_toptier_agency_abbreviation"
OR old."awarding_subtier_agency_abbreviation" IS DISTINCT FROM new."awarding_subtier_agency_abbreviation"
OR old."funding_subtier_agency_abbreviation" IS DISTINCT FROM new."funding_subtier_agency_abbreviation"
OR old."awarding_toptier_agency_agg_key" IS DISTINCT FROM new."awarding_toptier_agency_agg_key"
OR old."funding_toptier_agency_agg_key" IS DISTINCT FROM new."funding_toptier_agency_agg_key"
OR old."awarding_subtier_agency_agg_key" IS DISTINCT FROM new."awarding_subtier_agency_agg_key"
OR old."funding_subtier_agency_agg_key" IS DISTINCT FROM new."funding_subtier_agency_agg_key"
OR old."cfda_number" IS DISTINCT FROM new."cfda_number"
OR old."cfda_title" IS DISTINCT FROM new."cfda_title"
OR old."type_of_contract_pricing" IS DISTINCT FROM new."type_of_contract_pricing"
OR old."type_set_aside" IS DISTINCT FROM new."type_set_aside"
OR old."extent_competed" IS DISTINCT FROM new."extent_competed"
OR old."type" IS DISTINCT FROM new."type"
OR old."pop_country_code" IS DISTINCT FROM new."pop_country_code"
OR old."pop_country_name" IS DISTINCT FROM new."pop_country_name"
OR old."pop_state_code" IS DISTINCT FROM new."pop_state_code"
OR old."pop_county_code" IS DISTINCT FROM new."pop_county_code"
OR old."pop_county_name" IS DISTINCT FROM new."pop_county_name"
OR old."pop_zip5" IS DISTINCT FROM new."pop_zip5"
OR old."pop_congressional_code" IS DISTINCT FROM new."pop_congressional_code"
OR old."pop_city_name" IS DISTINCT FROM new."pop_city_name"
OR old."pop_county_agg_key" IS DISTINCT FROM new."pop_county_agg_key"
OR old."pop_congressional_agg_key" IS DISTINCT FROM new."pop_congressional_agg_key"
OR old."pop_state_agg_key" IS DISTINCT FROM new."pop_state_agg_key"
OR old."pop_country_agg_key" IS DISTINCT FROM new."pop_country_agg_key"
OR old."recipient_location_country_code" IS DISTINCT FROM new."recipient_location_country_code"
OR old."recipient_location_country_name" IS DISTINCT FROM new."recipient_location_country_name"
OR old."recipient_location_state_code" IS DISTINCT FROM new."recipient_location_state_code"
OR old."recipient_location_county_code" IS DISTINCT FROM new."recipient_location_county_code"
OR old."recipient_location_county_name" IS DISTINCT FROM new."recipient_location_county_name"
OR old."recipient_location_zip5" IS DISTINCT FROM new."recipient_location_zip5"
OR old."recipient_location_congressional_code" IS DISTINCT FROM new."recipient_location_congressional_code"
OR old."recipient_location_city_name" IS DISTINCT FROM new."recipient_location_city_name"
OR old."recipient_location_county_agg_key" IS DISTINCT FROM new."recipient_location_county_agg_key"
OR old."recipient_location_congressional_agg_key" IS DISTINCT FROM new."recipient_location_congressional_agg_key"
OR old."recipient_location_state_agg_key" IS DISTINCT FROM new."recipient_location_state_agg_key"
OR old."tas_paths" IS DISTINCT FROM new."tas_paths"
OR old."tas_components" IS DISTINCT FROM new."tas_components"
OR old."federal_accounts" IS DISTINCT FROM new."federal_accounts"
OR old."business_categories" IS DISTINCT FROM new."business_categories"
OR old."disaster_emergency_fund_codes" IS DISTINCT FROM new."disaster_emergency_fund_codes"
--LIMIT 1000
;
