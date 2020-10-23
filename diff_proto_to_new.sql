SELECT
    new.transaction_id,
    jsonb_strip_nulls(
        jsonb_build_object(
            'transaction_id', CASE WHEN new."transaction_id" IS DISTINCT FROM proto."transaction_id" THEN jsonb_build_object('new', new."transaction_id", 'proto', proto."transaction_id") ELSE null END,
            'detached_award_proc_unique', CASE WHEN new."detached_award_proc_unique" IS DISTINCT FROM proto."detached_award_proc_unique" THEN jsonb_build_object('new', new."detached_award_proc_unique", 'proto', proto."detached_award_proc_unique") ELSE null END,
            'afa_generated_unique', CASE WHEN new."afa_generated_unique" IS DISTINCT FROM proto."afa_generated_unique" THEN jsonb_build_object('new', new."afa_generated_unique", 'proto', proto."afa_generated_unique") ELSE null END,
            'update_date', CASE WHEN new."update_date" IS DISTINCT FROM proto."update_date" THEN jsonb_build_object('new', new."update_date", 'proto', proto."update_date") ELSE null END,
            'etl_update_date', CASE WHEN new."etl_update_date" IS DISTINCT FROM proto."etl_update_date" THEN jsonb_build_object('new', new."etl_update_date", 'proto', proto."etl_update_date") ELSE null END,
            'modification_number', CASE WHEN new."modification_number" IS DISTINCT FROM proto."modification_number" THEN jsonb_build_object('new', new."modification_number", 'proto', proto."modification_number") ELSE null END,
            'generated_unique_award_id', CASE WHEN new."generated_unique_award_id" IS DISTINCT FROM proto."generated_unique_award_id" THEN jsonb_build_object('new', new."generated_unique_award_id", 'proto', proto."generated_unique_award_id") ELSE null END,
            'award_id', CASE WHEN new."award_id" IS DISTINCT FROM proto."award_id" THEN jsonb_build_object('new', new."award_id", 'proto', proto."award_id") ELSE null END,
            'piid', CASE WHEN new."piid" IS DISTINCT FROM proto."piid" THEN jsonb_build_object('new', new."piid", 'proto', proto."piid") ELSE null END,
            'fain', CASE WHEN new."fain" IS DISTINCT FROM proto."fain" THEN jsonb_build_object('new', new."fain", 'proto', proto."fain") ELSE null END,
            'uri', CASE WHEN new."uri" IS DISTINCT FROM proto."uri" THEN jsonb_build_object('new', new."uri", 'proto', proto."uri") ELSE null END,
            'transaction_description', CASE WHEN new."transaction_description" IS DISTINCT FROM proto."transaction_description" THEN jsonb_build_object('new', new."transaction_description", 'proto', proto."transaction_description") ELSE null END,
            'product_or_service_code', CASE WHEN new."product_or_service_code" IS DISTINCT FROM proto."product_or_service_code" THEN jsonb_build_object('new', new."product_or_service_code", 'proto', proto."product_or_service_code") ELSE null END,
            'product_or_service_description', CASE WHEN new."product_or_service_description" IS DISTINCT FROM proto."product_or_service_description" THEN jsonb_build_object('new', new."product_or_service_description", 'proto', proto."product_or_service_description") ELSE null END,
            'psc_agg_key', CASE WHEN new."psc_agg_key" IS DISTINCT FROM proto."psc_agg_key" THEN jsonb_build_object('new', new."psc_agg_key", 'proto', proto."psc_agg_key") ELSE null END,
            'naics_code', CASE WHEN new."naics_code" IS DISTINCT FROM proto."naics_code" THEN jsonb_build_object('new', new."naics_code", 'proto', proto."naics_code") ELSE null END,
            'naics_description', CASE WHEN new."naics_description" IS DISTINCT FROM proto."naics_description" THEN jsonb_build_object('new', new."naics_description", 'proto', proto."naics_description") ELSE null END,
            'naics_agg_key', CASE WHEN new."naics_agg_key" IS DISTINCT FROM proto."naics_agg_key" THEN jsonb_build_object('new', new."naics_agg_key", 'proto', proto."naics_agg_key") ELSE null END,
            'type_description', CASE WHEN new."type_description" IS DISTINCT FROM proto."type_description" THEN jsonb_build_object('new', new."type_description", 'proto', proto."type_description") ELSE null END,
            'award_category', CASE WHEN new."award_category" IS DISTINCT FROM proto."award_category" THEN jsonb_build_object('new', new."award_category", 'proto', proto."award_category") ELSE null END,
            'recipient_unique_id', CASE WHEN new."recipient_unique_id" IS DISTINCT FROM proto."recipient_unique_id" THEN jsonb_build_object('new', new."recipient_unique_id", 'proto', proto."recipient_unique_id") ELSE null END,
            'recipient_name', CASE WHEN new."recipient_name" IS DISTINCT FROM proto."recipient_name" THEN jsonb_build_object('new', new."recipient_name", 'proto', proto."recipient_name") ELSE null END,
            'recipient_hash', CASE WHEN new."recipient_hash" IS DISTINCT FROM proto."recipient_hash" THEN jsonb_build_object('new', new."recipient_hash", 'proto', proto."recipient_hash") ELSE null END,
            'recipient_agg_key', CASE WHEN new."recipient_agg_key" IS DISTINCT FROM proto."recipient_agg_key" THEN jsonb_build_object('new', new."recipient_agg_key", 'proto', proto."recipient_agg_key") ELSE null END,
            'parent_recipient_unique_id', CASE WHEN new."parent_recipient_unique_id" IS DISTINCT FROM proto."parent_recipient_unique_id" THEN jsonb_build_object('new', new."parent_recipient_unique_id", 'proto', proto."parent_recipient_unique_id") ELSE null END,
            'parent_recipient_name', CASE WHEN new."parent_recipient_name" IS DISTINCT FROM proto."parent_recipient_name" THEN jsonb_build_object('new', new."parent_recipient_name", 'proto', proto."parent_recipient_name") ELSE null END
        ) || jsonb_build_object(
            'parent_recipient_hash', CASE WHEN new."parent_recipient_hash" IS DISTINCT FROM proto."parent_recipient_hash" THEN jsonb_build_object('new', new."parent_recipient_hash", 'proto', proto."parent_recipient_hash") ELSE null END,
            'action_date', CASE WHEN new."action_date" IS DISTINCT FROM proto."action_date" THEN jsonb_build_object('new', new."action_date", 'proto', proto."action_date") ELSE null END,
            'fiscal_action_date', CASE WHEN new."fiscal_action_date" IS DISTINCT FROM proto."fiscal_action_date" THEN jsonb_build_object('new', new."fiscal_action_date", 'proto', proto."fiscal_action_date") ELSE null END,
            'period_of_performance_start_date', CASE WHEN new."period_of_performance_start_date" IS DISTINCT FROM proto."period_of_performance_start_date" THEN jsonb_build_object('new', new."period_of_performance_start_date", 'proto', proto."period_of_performance_start_date") ELSE null END,
            'period_of_performance_current_end_date', CASE WHEN new."period_of_performance_current_end_date" IS DISTINCT FROM proto."period_of_performance_current_end_date" THEN jsonb_build_object('new', new."period_of_performance_current_end_date", 'proto', proto."period_of_performance_current_end_date") ELSE null END,
            'ordering_period_end_date', CASE WHEN new."ordering_period_end_date" IS DISTINCT FROM proto."ordering_period_end_date" THEN jsonb_build_object('new', new."ordering_period_end_date", 'proto', proto."ordering_period_end_date") ELSE null END,
            'fiscal_year', CASE WHEN new."fiscal_year" IS DISTINCT FROM proto."fiscal_year" THEN jsonb_build_object('new', new."fiscal_year", 'proto', proto."fiscal_year") ELSE null END,
            'award_fiscal_year', CASE WHEN new."award_fiscal_year" IS DISTINCT FROM proto."award_fiscal_year" THEN jsonb_build_object('new', new."award_fiscal_year", 'proto', proto."award_fiscal_year") ELSE null END,
            'award_amount', CASE WHEN new."award_amount" IS DISTINCT FROM proto."award_amount" THEN jsonb_build_object('new', new."award_amount", 'proto', proto."award_amount") ELSE null END,
            'federal_action_obligation', CASE WHEN new."federal_action_obligation" IS DISTINCT FROM proto."federal_action_obligation" THEN jsonb_build_object('new', new."federal_action_obligation", 'proto', proto."federal_action_obligation") ELSE null END,
            'face_value_loan_guarantee', CASE WHEN new."face_value_loan_guarantee" IS DISTINCT FROM proto."face_value_loan_guarantee" THEN jsonb_build_object('new', new."face_value_loan_guarantee", 'proto', proto."face_value_loan_guarantee") ELSE null END,
            'original_loan_subsidy_cost', CASE WHEN new."original_loan_subsidy_cost" IS DISTINCT FROM proto."original_loan_subsidy_cost" THEN jsonb_build_object('new', new."original_loan_subsidy_cost", 'proto', proto."original_loan_subsidy_cost") ELSE null END,
            'generated_pragmatic_obligation', CASE WHEN new."generated_pragmatic_obligation" IS DISTINCT FROM proto."generated_pragmatic_obligation" THEN jsonb_build_object('new', new."generated_pragmatic_obligation", 'proto', proto."generated_pragmatic_obligation") ELSE null END,
            'awarding_agency_id', CASE WHEN new."awarding_agency_id" IS DISTINCT FROM proto."awarding_agency_id" THEN jsonb_build_object('new', new."awarding_agency_id", 'proto', proto."awarding_agency_id") ELSE null END,
            'funding_agency_id', CASE WHEN new."funding_agency_id" IS DISTINCT FROM proto."funding_agency_id" THEN jsonb_build_object('new', new."funding_agency_id", 'proto', proto."funding_agency_id") ELSE null END,
            'awarding_toptier_agency_name', CASE WHEN new."awarding_toptier_agency_name" IS DISTINCT FROM proto."awarding_toptier_agency_name" THEN jsonb_build_object('new', new."awarding_toptier_agency_name", 'proto', proto."awarding_toptier_agency_name") ELSE null END,
            'funding_toptier_agency_name', CASE WHEN new."funding_toptier_agency_name" IS DISTINCT FROM proto."funding_toptier_agency_name" THEN jsonb_build_object('new', new."funding_toptier_agency_name", 'proto', proto."funding_toptier_agency_name") ELSE null END,
            'awarding_subtier_agency_name', CASE WHEN new."awarding_subtier_agency_name" IS DISTINCT FROM proto."awarding_subtier_agency_name" THEN jsonb_build_object('new', new."awarding_subtier_agency_name", 'proto', proto."awarding_subtier_agency_name") ELSE null END,
            'funding_subtier_agency_name', CASE WHEN new."funding_subtier_agency_name" IS DISTINCT FROM proto."funding_subtier_agency_name" THEN jsonb_build_object('new', new."funding_subtier_agency_name", 'proto', proto."funding_subtier_agency_name") ELSE null END,
            'awarding_toptier_agency_abbreviation', CASE WHEN new."awarding_toptier_agency_abbreviation" IS DISTINCT FROM proto."awarding_toptier_agency_abbreviation" THEN jsonb_build_object('new', new."awarding_toptier_agency_abbreviation", 'proto', proto."awarding_toptier_agency_abbreviation") ELSE null END,
            'funding_toptier_agency_abbreviation', CASE WHEN new."funding_toptier_agency_abbreviation" IS DISTINCT FROM proto."funding_toptier_agency_abbreviation" THEN jsonb_build_object('new', new."funding_toptier_agency_abbreviation", 'proto', proto."funding_toptier_agency_abbreviation") ELSE null END,
            'awarding_subtier_agency_abbreviation', CASE WHEN new."awarding_subtier_agency_abbreviation" IS DISTINCT FROM proto."awarding_subtier_agency_abbreviation" THEN jsonb_build_object('new', new."awarding_subtier_agency_abbreviation", 'proto', proto."awarding_subtier_agency_abbreviation") ELSE null END,
            'funding_subtier_agency_abbreviation', CASE WHEN new."funding_subtier_agency_abbreviation" IS DISTINCT FROM proto."funding_subtier_agency_abbreviation" THEN jsonb_build_object('new', new."funding_subtier_agency_abbreviation", 'proto', proto."funding_subtier_agency_abbreviation") ELSE null END,
            'awarding_toptier_agency_agg_key', CASE WHEN new."awarding_toptier_agency_agg_key" IS DISTINCT FROM proto."awarding_toptier_agency_agg_key" THEN jsonb_build_object('new', new."awarding_toptier_agency_agg_key", 'proto', proto."awarding_toptier_agency_agg_key") ELSE null END,
            'funding_toptier_agency_agg_key', CASE WHEN new."funding_toptier_agency_agg_key" IS DISTINCT FROM proto."funding_toptier_agency_agg_key" THEN jsonb_build_object('new', new."funding_toptier_agency_agg_key", 'proto', proto."funding_toptier_agency_agg_key") ELSE null END
        ) || jsonb_build_object(
            'awarding_subtier_agency_agg_key', CASE WHEN new."awarding_subtier_agency_agg_key" IS DISTINCT FROM proto."awarding_subtier_agency_agg_key" THEN jsonb_build_object('new', new."awarding_subtier_agency_agg_key", 'proto', proto."awarding_subtier_agency_agg_key") ELSE null END,
            'funding_subtier_agency_agg_key', CASE WHEN new."funding_subtier_agency_agg_key" IS DISTINCT FROM proto."funding_subtier_agency_agg_key" THEN jsonb_build_object('new', new."funding_subtier_agency_agg_key", 'proto', proto."funding_subtier_agency_agg_key") ELSE null END,
            'cfda_number', CASE WHEN new."cfda_number" IS DISTINCT FROM proto."cfda_number" THEN jsonb_build_object('new', new."cfda_number", 'proto', proto."cfda_number") ELSE null END,
            'cfda_title', CASE WHEN new."cfda_title" IS DISTINCT FROM proto."cfda_title" THEN jsonb_build_object('new', new."cfda_title", 'proto', proto."cfda_title") ELSE null END,
            'type_of_contract_pricing', CASE WHEN new."type_of_contract_pricing" IS DISTINCT FROM proto."type_of_contract_pricing" THEN jsonb_build_object('new', new."type_of_contract_pricing", 'proto', proto."type_of_contract_pricing") ELSE null END,
            'type_set_aside', CASE WHEN new."type_set_aside" IS DISTINCT FROM proto."type_set_aside" THEN jsonb_build_object('new', new."type_set_aside", 'proto', proto."type_set_aside") ELSE null END,
            'extent_competed', CASE WHEN new."extent_competed" IS DISTINCT FROM proto."extent_competed" THEN jsonb_build_object('new', new."extent_competed", 'proto', proto."extent_competed") ELSE null END,
            'type', CASE WHEN new."type" IS DISTINCT FROM proto."type" THEN jsonb_build_object('new', new."type", 'proto', proto."type") ELSE null END,
            'pop_country_code', CASE WHEN new."pop_country_code" IS DISTINCT FROM proto."pop_country_code" THEN jsonb_build_object('new', new."pop_country_code", 'proto', proto."pop_country_code") ELSE null END,
            'pop_country_name', CASE WHEN new."pop_country_name" IS DISTINCT FROM proto."pop_country_name" THEN jsonb_build_object('new', new."pop_country_name", 'proto', proto."pop_country_name") ELSE null END,
            'pop_state_code', CASE WHEN new."pop_state_code" IS DISTINCT FROM proto."pop_state_code" THEN jsonb_build_object('new', new."pop_state_code", 'proto', proto."pop_state_code") ELSE null END,
            'pop_county_code', CASE WHEN new."pop_county_code" IS DISTINCT FROM proto."pop_county_code" THEN jsonb_build_object('new', new."pop_county_code", 'proto', proto."pop_county_code") ELSE null END,
            'pop_county_name', CASE WHEN new."pop_county_name" IS DISTINCT FROM proto."pop_county_name" THEN jsonb_build_object('new', new."pop_county_name", 'proto', proto."pop_county_name") ELSE null END,
            'pop_zip5', CASE WHEN new."pop_zip5" IS DISTINCT FROM proto."pop_zip5" THEN jsonb_build_object('new', new."pop_zip5", 'proto', proto."pop_zip5") ELSE null END,
            'pop_congressional_code', CASE WHEN new."pop_congressional_code" IS DISTINCT FROM proto."pop_congressional_code" THEN jsonb_build_object('new', new."pop_congressional_code", 'proto', proto."pop_congressional_code") ELSE null END,
            'pop_city_name', CASE WHEN new."pop_city_name" IS DISTINCT FROM proto."pop_city_name" THEN jsonb_build_object('new', new."pop_city_name", 'proto', proto."pop_city_name") ELSE null END,
            'pop_county_agg_key', CASE WHEN new."pop_county_agg_key" IS DISTINCT FROM proto."pop_county_agg_key" THEN jsonb_build_object('new', new."pop_county_agg_key", 'proto', proto."pop_county_agg_key") ELSE null END,
            'pop_congressional_agg_key', CASE WHEN new."pop_congressional_agg_key" IS DISTINCT FROM proto."pop_congressional_agg_key" THEN jsonb_build_object('new', new."pop_congressional_agg_key", 'proto', proto."pop_congressional_agg_key") ELSE null END,
            'pop_state_agg_key', CASE WHEN new."pop_state_agg_key" IS DISTINCT FROM proto."pop_state_agg_key" THEN jsonb_build_object('new', new."pop_state_agg_key", 'proto', proto."pop_state_agg_key") ELSE null END,
            'pop_country_agg_key', CASE WHEN new."pop_country_agg_key" IS DISTINCT FROM proto."pop_country_agg_key" THEN jsonb_build_object('new', new."pop_country_agg_key", 'proto', proto."pop_country_agg_key") ELSE null END,
            'recipient_location_country_code', CASE WHEN new."recipient_location_country_code" IS DISTINCT FROM proto."recipient_location_country_code" THEN jsonb_build_object('new', new."recipient_location_country_code", 'proto', proto."recipient_location_country_code") ELSE null END,
            'recipient_location_country_name', CASE WHEN new."recipient_location_country_name" IS DISTINCT FROM proto."recipient_location_country_name" THEN jsonb_build_object('new', new."recipient_location_country_name", 'proto', proto."recipient_location_country_name") ELSE null END,
            'recipient_location_state_code', CASE WHEN new."recipient_location_state_code" IS DISTINCT FROM proto."recipient_location_state_code" THEN jsonb_build_object('new', new."recipient_location_state_code", 'proto', proto."recipient_location_state_code") ELSE null END,
            'recipient_location_county_code', CASE WHEN new."recipient_location_county_code" IS DISTINCT FROM proto."recipient_location_county_code" THEN jsonb_build_object('new', new."recipient_location_county_code", 'proto', proto."recipient_location_county_code") ELSE null END
        ) || jsonb_build_object(
            'recipient_location_county_name', CASE WHEN new."recipient_location_county_name" IS DISTINCT FROM proto."recipient_location_county_name" THEN jsonb_build_object('new', new."recipient_location_county_name", 'proto', proto."recipient_location_county_name") ELSE null END,
            'recipient_location_zip5', CASE WHEN new."recipient_location_zip5" IS DISTINCT FROM proto."recipient_location_zip5" THEN jsonb_build_object('new', new."recipient_location_zip5", 'proto', proto."recipient_location_zip5") ELSE null END,
            'recipient_location_congressional_code', CASE WHEN new."recipient_location_congressional_code" IS DISTINCT FROM proto."recipient_location_congressional_code" THEN jsonb_build_object('new', new."recipient_location_congressional_code", 'proto', proto."recipient_location_congressional_code") ELSE null END,
            'recipient_location_city_name', CASE WHEN new."recipient_location_city_name" IS DISTINCT FROM proto."recipient_location_city_name" THEN jsonb_build_object('new', new."recipient_location_city_name", 'proto', proto."recipient_location_city_name") ELSE null END,
            'recipient_location_county_agg_key', CASE WHEN new."recipient_location_county_agg_key" IS DISTINCT FROM proto."recipient_location_county_agg_key" THEN jsonb_build_object('new', new."recipient_location_county_agg_key", 'proto', proto."recipient_location_county_agg_key") ELSE null END,
            'recipient_location_congressional_agg_key', CASE WHEN new."recipient_location_congressional_agg_key" IS DISTINCT FROM proto."recipient_location_congressional_agg_key" THEN jsonb_build_object('new', new."recipient_location_congressional_agg_key", 'proto', proto."recipient_location_congressional_agg_key") ELSE null END,
            'recipient_location_state_agg_key', CASE WHEN new."recipient_location_state_agg_key" IS DISTINCT FROM proto."recipient_location_state_agg_key" THEN jsonb_build_object('new', new."recipient_location_state_agg_key", 'proto', proto."recipient_location_state_agg_key") ELSE null END,
            'tas_paths', CASE WHEN new."tas_paths" IS DISTINCT FROM proto."tas_paths" THEN jsonb_build_object('new', new."tas_paths", 'proto', proto."tas_paths") ELSE null END,
            'tas_components', CASE WHEN new."tas_components" IS DISTINCT FROM proto."tas_components" THEN jsonb_build_object('new', new."tas_components", 'proto', proto."tas_components") ELSE null END,
            'federal_accounts', CASE WHEN new."federal_accounts" IS DISTINCT FROM proto."federal_accounts" THEN jsonb_build_object('new', new."federal_accounts", 'proto', proto."federal_accounts") ELSE null END,
            'business_categories', CASE WHEN new."business_categories" IS DISTINCT FROM proto."business_categories" THEN jsonb_build_object('new', new."business_categories", 'proto', proto."business_categories") ELSE null END,
            'disaster_emergency_fund_codes', CASE WHEN new."disaster_emergency_fund_codes" IS DISTINCT FROM proto."disaster_emergency_fund_codes" THEN jsonb_build_object('new', new."disaster_emergency_fund_codes", 'proto', proto."disaster_emergency_fund_codes") ELSE null END
        )
    ) AS json_diff

FROM universal_transaction_matview AS new
LEFT JOIN universal_transaction_matview_proto AS proto USING (transaction_id)
WHERE
new."transaction_id" IS DISTINCT FROM proto."transaction_id"
OR new."detached_award_proc_unique" IS DISTINCT FROM proto."detached_award_proc_unique"
OR new."afa_generated_unique" IS DISTINCT FROM proto."afa_generated_unique"
OR new."update_date" IS DISTINCT FROM proto."update_date"
OR new."etl_update_date" IS DISTINCT FROM proto."etl_update_date"
OR new."modification_number" IS DISTINCT FROM proto."modification_number"
OR new."generated_unique_award_id" IS DISTINCT FROM proto."generated_unique_award_id"
OR new."award_id" IS DISTINCT FROM proto."award_id"
OR new."piid" IS DISTINCT FROM proto."piid"
OR new."fain" IS DISTINCT FROM proto."fain"
OR new."uri" IS DISTINCT FROM proto."uri"
OR new."transaction_description" IS DISTINCT FROM proto."transaction_description"
OR new."product_or_service_code" IS DISTINCT FROM proto."product_or_service_code"
OR new."product_or_service_description" IS DISTINCT FROM proto."product_or_service_description"
OR new."psc_agg_key" IS DISTINCT FROM proto."psc_agg_key"
OR new."naics_code" IS DISTINCT FROM proto."naics_code"
OR new."naics_description" IS DISTINCT FROM proto."naics_description"
OR new."naics_agg_key" IS DISTINCT FROM proto."naics_agg_key"
OR new."type_description" IS DISTINCT FROM proto."type_description"
OR new."award_category" IS DISTINCT FROM proto."award_category"
OR new."recipient_unique_id" IS DISTINCT FROM proto."recipient_unique_id"
OR new."recipient_name" IS DISTINCT FROM proto."recipient_name"
OR new."recipient_hash" IS DISTINCT FROM proto."recipient_hash"
OR new."recipient_agg_key" IS DISTINCT FROM proto."recipient_agg_key"
OR new."parent_recipient_unique_id" IS DISTINCT FROM proto."parent_recipient_unique_id"
OR new."parent_recipient_name" IS DISTINCT FROM proto."parent_recipient_name"
OR new."parent_recipient_hash" IS DISTINCT FROM proto."parent_recipient_hash"
OR new."action_date" IS DISTINCT FROM proto."action_date"
OR new."fiscal_action_date" IS DISTINCT FROM proto."fiscal_action_date"
OR new."period_of_performance_start_date" IS DISTINCT FROM proto."period_of_performance_start_date"
OR new."period_of_performance_current_end_date" IS DISTINCT FROM proto."period_of_performance_current_end_date"
OR new."ordering_period_end_date" IS DISTINCT FROM proto."ordering_period_end_date"
OR new."fiscal_year" IS DISTINCT FROM proto."fiscal_year"
OR new."award_fiscal_year" IS DISTINCT FROM proto."award_fiscal_year"
OR new."award_amount" IS DISTINCT FROM proto."award_amount"
OR new."federal_action_obligation" IS DISTINCT FROM proto."federal_action_obligation"
OR new."face_value_loan_guarantee" IS DISTINCT FROM proto."face_value_loan_guarantee"
OR new."original_loan_subsidy_cost" IS DISTINCT FROM proto."original_loan_subsidy_cost"
OR new."generated_pragmatic_obligation" IS DISTINCT FROM proto."generated_pragmatic_obligation"
OR new."awarding_agency_id" IS DISTINCT FROM proto."awarding_agency_id"
OR new."funding_agency_id" IS DISTINCT FROM proto."funding_agency_id"
OR new."awarding_toptier_agency_name" IS DISTINCT FROM proto."awarding_toptier_agency_name"
OR new."funding_toptier_agency_name" IS DISTINCT FROM proto."funding_toptier_agency_name"
OR new."awarding_subtier_agency_name" IS DISTINCT FROM proto."awarding_subtier_agency_name"
OR new."funding_subtier_agency_name" IS DISTINCT FROM proto."funding_subtier_agency_name"
OR new."awarding_toptier_agency_abbreviation" IS DISTINCT FROM proto."awarding_toptier_agency_abbreviation"
OR new."funding_toptier_agency_abbreviation" IS DISTINCT FROM proto."funding_toptier_agency_abbreviation"
OR new."awarding_subtier_agency_abbreviation" IS DISTINCT FROM proto."awarding_subtier_agency_abbreviation"
OR new."funding_subtier_agency_abbreviation" IS DISTINCT FROM proto."funding_subtier_agency_abbreviation"
OR new."awarding_toptier_agency_agg_key" IS DISTINCT FROM proto."awarding_toptier_agency_agg_key"
OR new."funding_toptier_agency_agg_key" IS DISTINCT FROM proto."funding_toptier_agency_agg_key"
OR new."awarding_subtier_agency_agg_key" IS DISTINCT FROM proto."awarding_subtier_agency_agg_key"
OR new."funding_subtier_agency_agg_key" IS DISTINCT FROM proto."funding_subtier_agency_agg_key"
OR new."cfda_number" IS DISTINCT FROM proto."cfda_number"
OR new."cfda_title" IS DISTINCT FROM proto."cfda_title"
OR new."type_of_contract_pricing" IS DISTINCT FROM proto."type_of_contract_pricing"
OR new."type_set_aside" IS DISTINCT FROM proto."type_set_aside"
OR new."extent_competed" IS DISTINCT FROM proto."extent_competed"
OR new."type" IS DISTINCT FROM proto."type"
OR new."pop_country_code" IS DISTINCT FROM proto."pop_country_code"
OR new."pop_country_name" IS DISTINCT FROM proto."pop_country_name"
OR new."pop_state_code" IS DISTINCT FROM proto."pop_state_code"
OR new."pop_county_code" IS DISTINCT FROM proto."pop_county_code"
OR new."pop_county_name" IS DISTINCT FROM proto."pop_county_name"
OR new."pop_zip5" IS DISTINCT FROM proto."pop_zip5"
OR new."pop_congressional_code" IS DISTINCT FROM proto."pop_congressional_code"
OR new."pop_city_name" IS DISTINCT FROM proto."pop_city_name"
OR new."pop_county_agg_key" IS DISTINCT FROM proto."pop_county_agg_key"
OR new."pop_congressional_agg_key" IS DISTINCT FROM proto."pop_congressional_agg_key"
OR new."pop_state_agg_key" IS DISTINCT FROM proto."pop_state_agg_key"
OR new."pop_country_agg_key" IS DISTINCT FROM proto."pop_country_agg_key"
OR new."recipient_location_country_code" IS DISTINCT FROM proto."recipient_location_country_code"
OR new."recipient_location_country_name" IS DISTINCT FROM proto."recipient_location_country_name"
OR new."recipient_location_state_code" IS DISTINCT FROM proto."recipient_location_state_code"
OR new."recipient_location_county_code" IS DISTINCT FROM proto."recipient_location_county_code"
OR new."recipient_location_county_name" IS DISTINCT FROM proto."recipient_location_county_name"
OR new."recipient_location_zip5" IS DISTINCT FROM proto."recipient_location_zip5"
OR new."recipient_location_congressional_code" IS DISTINCT FROM proto."recipient_location_congressional_code"
OR new."recipient_location_city_name" IS DISTINCT FROM proto."recipient_location_city_name"
OR new."recipient_location_county_agg_key" IS DISTINCT FROM proto."recipient_location_county_agg_key"
OR new."recipient_location_congressional_agg_key" IS DISTINCT FROM proto."recipient_location_congressional_agg_key"
OR new."recipient_location_state_agg_key" IS DISTINCT FROM proto."recipient_location_state_agg_key"
OR new."tas_paths" IS DISTINCT FROM proto."tas_paths"
OR new."tas_components" IS DISTINCT FROM proto."tas_components"
OR new."federal_accounts" IS DISTINCT FROM proto."federal_accounts"
OR new."business_categories" IS DISTINCT FROM proto."business_categories"
OR new."disaster_emergency_fund_codes" IS DISTINCT FROM proto."disaster_emergency_fund_codes"
--LIMIT 1000
;
