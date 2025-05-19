from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

# The order of these fields should always match the order of the
# SELECT statement in "transaction_search_load_sql_string"
TRANSACTION_SEARCH_COLUMNS = {
    # Keys
    "transaction_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL", "gold": False},
    "award_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL", "gold": False},
    # while transaction_unique_id is gold, it can't be NULL
    "transaction_unique_id": {"delta": "STRING NOT NULL", "postgres": "TEXT NOT NULL", "gold": False},
    "usaspending_unique_transaction_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "modification_number": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "generated_unique_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Dates
    "action_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "fiscal_action_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "last_modified_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "award_certified_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "award_fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "create_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": True},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": False},
    "award_update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": False},
    "award_date_signed": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "etl_update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": False},
    "period_of_performance_start_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "period_of_performance_current_end_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "initial_report_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    # Agencies
    "awarding_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_toptier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "funding_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "awarding_sub_tier_agency_c": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "funding_sub_tier_agency_co": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_subtier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "awarding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "funding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "awarding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_office_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_office_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_office_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_office_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Typing
    # while is_fpds is gold, it also can't be NULL
    "is_fpds": {"delta": "BOOLEAN NOT NULL", "postgres": "BOOLEAN NOT NULL", "gold": False},
    "type_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "action_type": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "action_type_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "award_category": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "transaction_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "business_categories": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    # Amounts
    "award_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "generated_pragmatic_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "federal_action_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "original_loan_subsidy_cost": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "face_value_loan_guarantee": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "indirect_federal_sharing": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "funding_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "total_funding_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "non_federal_funding_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    # Recipient
    "recipient_hash": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_levels": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "recipient_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_hash": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Recipient Location
    "recipient_location_country_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_country_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_county_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_county_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_county_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_congressional_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_congressional_code_current": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "legal_entity_zip4": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_zip_last4": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_city_code": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_city_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "legal_entity_address_line1": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_address_line2": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_address_line3": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_foreign_city": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_foreign_descr": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_foreign_posta": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "legal_entity_foreign_provi": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_county_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Place of Performance
    "place_of_performance_code": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "place_of_performance_scope": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_country_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_county_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_congressional_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_congressional_code_current": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pop_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "place_of_performance_zip4a": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "place_of_perform_zip_last4": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pop_city_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "place_of_performance_forei": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pop_county_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Accounts
    "treasury_account_identifiers": {"delta": "ARRAY<INTEGER>", "postgres": "TEXT[]", "gold": False},
    "tas_paths": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "tas_components": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "federal_accounts": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "disaster_emergency_fund_codes": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    # Officer Amounts
    "officer_1_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_1_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "officer_2_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_2_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "officer_3_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_3_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "officer_4_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_4_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "officer_5_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_5_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": True},
    # Exclusively FABS
    "published_fabs_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "afa_generated_unique": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "business_funds_ind_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "business_funds_indicator": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "business_types": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "business_types_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "cfda_number": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "cfda_title": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "cfda_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "correction_delete_indicatr": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "correction_delete_ind_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fain": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_opportunity_goals": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "funding_opportunity_number": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "record_type": {"delta": "INTEGER", "postgres": "INTEGER", "gold": True},
    "record_type_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "sai_number": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "uri": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    # Exclusively FPDS
    "detached_award_procurement_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "detached_award_proc_unique": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "a_76_fair_act_action": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "a_76_fair_act_action_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "agency_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "airport_authority": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "alaskan_native_owned_corpo": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "alaskan_native_servicing_i": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "american_indian_owned_busi": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "asian_pacific_american_own": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "base_and_all_options_value": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "base_exercised_options_val": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "black_american_owned_busin": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "c1862_land_grant_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "c1890_land_grant_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "c1994_land_grant_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "c8a_program_participant": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "cage_code": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "city_local_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "clinger_cohen_act_planning": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "clinger_cohen_act_pla_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "commercial_item_acqui_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "commercial_item_acquisitio": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "commercial_item_test_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "commercial_item_test_progr": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "community_developed_corpor": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "community_development_corp": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "consolidated_contract": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "consolidated_contract_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "construction_wage_rat_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "construction_wage_rate_req": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contingency_humanitar_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contingency_humanitarian_o": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_award_type": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_award_type_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_bundling": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_bundling_descrip": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_financing": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contract_financing_descrip": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contracting_officers_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contracting_officers_deter": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "contracts": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "corporate_entity_not_tax_e": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "corporate_entity_tax_exemp": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "cost_accounting_stand_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "cost_accounting_standards": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "cost_or_pricing_data": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "cost_or_pricing_data_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "council_of_governments": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "country_of_product_or_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "country_of_product_or_serv": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "county_local_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "current_total_value_award": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "dod_claimant_prog_cod_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "dod_claimant_program_code": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "domestic_or_foreign_e_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "domestic_or_foreign_entity": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "domestic_shelter": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "dot_certified_disadvantage": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "economically_disadvantaged": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "educational_institution": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "emerging_small_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "epa_designated_produc_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "epa_designated_product": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "evaluated_preference": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "evaluated_preference_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "extent_competed": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "extent_compete_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fair_opportunity_limi_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fair_opportunity_limited_s": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fed_biz_opps": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fed_biz_opps_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "federal_agency": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "federally_funded_research": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "for_profit_organization": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "foreign_funding": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "foreign_funding_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "foreign_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "foreign_owned_and_located": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "foundation": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "government_furnished_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "government_furnished_prope": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "grants": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "hispanic_american_owned_bu": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "hispanic_servicing_institu": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "historically_black_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "historically_underutilized": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "hospital_flag": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "housing_authorities_public": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "idv_type": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "idv_type_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "indian_tribe_federally_rec": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "information_technolog_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "information_technology_com": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "inherently_government_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "inherently_government_func": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "inter_municipal_local_gove": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "interagency_contract_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "interagency_contracting_au": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "international_organization": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "interstate_entity": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "joint_venture_economically": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "joint_venture_women_owned": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "labor_standards": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "labor_standards_descrip": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "labor_surplus_area_firm": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "limited_liability_corporat": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "local_area_set_aside": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "local_area_set_aside_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "local_government_owned": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "major_program": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "manufacturer_of_goods": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "materials_supplies_article": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "materials_supplies_descrip": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "minority_institution": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "minority_owned_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "multi_year_contract": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "multi_year_contract_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "multiple_or_single_aw_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "multiple_or_single_award_i": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "municipality_local_governm": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "naics_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "naics_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "national_interest_action": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "national_interest_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "native_american_owned_busi": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "native_hawaiian_owned_busi": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "native_hawaiian_servicing": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "nonprofit_organization": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "number_of_actions": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "number_of_offers_received": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "ordering_period_end_date": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "organizational_type": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "other_minority_owned_busin": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "other_not_for_profit_organ": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "other_statutory_authority": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "other_than_full_and_o_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "other_than_full_and_open_c": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "parent_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "partnership_or_limited_lia": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "performance_based_se_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "performance_based_service": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "period_of_perf_potential_e": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "piid": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "place_of_manufacture": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "place_of_manufacture_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "planning_commission": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "port_authority": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "potential_total_value_awar": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "price_evaluation_adjustmen": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "private_university_or_coll": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "product_or_service_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "product_or_service_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "program_acronym": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "program_system_or_equ_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "program_system_or_equipmen": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pulled_from": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "purchase_card_as_paym_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "purchase_card_as_payment_m": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "receives_contracts_and_gra": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "recovered_materials_s_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recovered_materials_sustai": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_idv_agency_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_idv_agency_iden": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_idv_modificatio": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_idv_type": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_idv_type_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_mult_or_si_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "referenced_mult_or_single": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "research": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "research_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "sam_exception": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "sam_exception_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "sba_certified_8_a_joint_ve": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "school_district_local_gove": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "school_of_forestry": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "sea_transportation": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "sea_transportation_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "self_certified_small_disad": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "service_disabled_veteran_o": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "small_agricultural_coopera": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "small_business_competitive": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "small_disadvantaged_busine": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "sole_proprietorship": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "solicitation_date": {"delta": "DATE", "postgres": "DATE", "gold": True},
    "solicitation_identifier": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "solicitation_procedur_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "solicitation_procedures": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "state_controlled_instituti": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "subchapter_s_corporation": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "subcontinent_asian_asian_i": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "subcontracting_plan": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "subcontracting_plan_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "the_ability_one_program": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "total_obligated_amount": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "township_local_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "transaction_number": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "transit_authority": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "tribal_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "tribally_owned_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "type_of_contract_pricing": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_of_contract_pric_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "type_of_idc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "type_of_idc_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "type_set_aside": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_set_aside_description": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "undefinitized_action": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "undefinitized_action_desc": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "us_federal_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "us_government_entity": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "us_local_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "us_state_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "us_tribal_government": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "vendor_doing_as_business_n": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "vendor_fax_number": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "vendor_phone_number": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "veteran_owned_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "veterinary_college": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "veterinary_hospital": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "woman_owned_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "women_owned_small_business": {"delta": "BOOLEAN", "postgres": "BOOLEAN", "gold": True},
    "program_activities": {"delta": "STRING", "postgres": "JSONB", "gold": False},
}
TRANSACTION_SEARCH_DELTA_COLUMNS = {k: v["delta"] for k, v in TRANSACTION_SEARCH_COLUMNS.items() if not v["gold"]}
TRANSACTION_SEARCH_GOLD_DELTA_COLUMNS = {k: v["delta"] for k, v in TRANSACTION_SEARCH_COLUMNS.items()}
TRANSACTION_SEARCH_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in TRANSACTION_SEARCH_COLUMNS.items() if not v["gold"]}
TRANSACTION_SEARCH_POSTGRES_GOLD_COLUMNS = {k: v["postgres"] for k, v in TRANSACTION_SEARCH_COLUMNS.items()}

ALL_AWARD_TYPES = list(award_type_mapping.keys())

transaction_search_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TRANSACTION_SEARCH_GOLD_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""

_base_load_sql_string = rf"""
    SELECT
        -- Keys
        transaction_normalized.id AS transaction_id,
        transaction_normalized.award_id,
        transaction_normalized.transaction_unique_id,
        transaction_normalized.usaspending_unique_transaction_id,
        transaction_normalized.modification_number,
        awards.generated_unique_award_id,

        -- Dates
        DATE(transaction_normalized.action_date) AS action_date,
        DATE(transaction_normalized.action_date + interval '3 months') AS fiscal_action_date,
        DATE(transaction_normalized.last_modified_date) AS last_modified_date,
        transaction_normalized.fiscal_year,
        awards.certified_date AS award_certified_date,
        YEAR(awards.certified_date + interval '3 months') AS award_fiscal_year,
        transaction_normalized.create_date,
        transaction_normalized.update_date,
        awards.update_date AS award_update_date,
        DATE(awards.date_signed) AS award_date_signed,
        GREATEST(transaction_normalized.update_date, awards.update_date) AS etl_update_date,
        transaction_normalized.period_of_performance_start_date,
        transaction_normalized.period_of_performance_current_end_date,
        COALESCE(
            CAST(transaction_fabs.created_at AS DATE),
            CAST(transaction_fpds.initial_report_date AS DATE)
        ) AS initial_report_date,

        -- Agencies
        COALESCE(transaction_fabs.awarding_agency_code, transaction_fpds.awarding_agency_code) AS awarding_agency_code,
        TAA.name AS awarding_toptier_agency_name,
        COALESCE(transaction_fabs.awarding_agency_name, transaction_fpds.awarding_agency_name) AS awarding_toptier_agency_name_raw,
        COALESCE(transaction_fabs.funding_agency_code, transaction_fpds.funding_agency_code) AS funding_agency_code,
        TFA.name AS funding_toptier_agency_name,
        COALESCE(transaction_fabs.funding_agency_name, transaction_fpds.funding_agency_name) AS funding_toptier_agency_name_raw,
        COALESCE(transaction_fabs.awarding_sub_tier_agency_c, transaction_fpds.awarding_sub_tier_agency_c) AS awarding_sub_tier_agency_c,
        SAA.name AS awarding_subtier_agency_name,
        COALESCE(transaction_fabs.awarding_sub_tier_agency_n, transaction_fpds.awarding_sub_tier_agency_n) AS awarding_subtier_agency_name_raw,
        COALESCE(transaction_fabs.funding_sub_tier_agency_co, transaction_fpds.funding_sub_tier_agency_co) AS funding_sub_tier_agency_co,
        SFA.name AS funding_subtier_agency_name,
        COALESCE(transaction_fabs.funding_sub_tier_agency_na, transaction_fpds.funding_sub_tier_agency_na) AS funding_subtier_agency_name_raw,
        AA_ID.id AS awarding_toptier_agency_id,
        FA_ID.id AS funding_toptier_agency_id,
        transaction_normalized.awarding_agency_id,
        transaction_normalized.funding_agency_id,
        TAA.abbreviation AS awarding_toptier_agency_abbreviation,
        TFA.abbreviation AS funding_toptier_agency_abbreviation,
        SAA.abbreviation AS awarding_subtier_agency_abbreviation,
        SFA.abbreviation AS funding_subtier_agency_abbreviation,
        COALESCE(transaction_fabs.awarding_office_code, transaction_fpds.awarding_office_code) AS awarding_office_code,
        COALESCE(awarding_office.office_name, transaction_fabs.awarding_office_name, transaction_fpds.awarding_office_name) AS awarding_office_name,
        COALESCE(transaction_fabs.funding_office_code, transaction_fpds.funding_office_code) AS funding_office_code,
        COALESCE(funding_office.office_name, transaction_fabs.funding_office_name, transaction_fpds.funding_office_name) AS funding_office_name,
        -- Typing
        transaction_normalized.is_fpds,
        transaction_normalized.type AS type_raw,
        transaction_normalized.type_description AS type_description_raw,
        CASE
            WHEN (
                transaction_normalized.type NOT IN ({", ".join([f"'{award_type}'" for award_type in ALL_AWARD_TYPES])}) OR transaction_normalized.type is NULL
            ) THEN '-1'
            ELSE transaction_normalized.type
        END AS type,
        CASE
            WHEN (
                transaction_normalized.type NOT IN ({", ".join([f"'{award_type}'" for award_type in ALL_AWARD_TYPES])}) OR transaction_normalized.type is NULL
            ) THEN 'NOT SPECIFIED'
            ELSE transaction_normalized.type_description
        END AS type_description,
        transaction_normalized.action_type,
        transaction_normalized.action_type_description,
        awards.category AS award_category,
        transaction_normalized.description AS transaction_description,
        transaction_normalized.business_categories,

        -- Amounts
        CAST(COALESCE(
            CASE
                WHEN transaction_normalized.type IN('07','08') THEN awards.total_subsidy_cost
                ELSE awards.total_obligation
            END,
            0
        ) AS NUMERIC(23, 2)) AS award_amount,
        CAST(COALESCE(
            CASE
                WHEN transaction_normalized.type IN('07','08') THEN transaction_normalized.original_loan_subsidy_cost
                ELSE transaction_normalized.federal_action_obligation
            END,
            0
        ) AS NUMERIC(23, 2)) AS generated_pragmatic_obligation,
        CAST(COALESCE(transaction_normalized.federal_action_obligation, 0) AS NUMERIC(23, 2))
            AS federal_action_obligation,
        CAST(COALESCE(transaction_normalized.original_loan_subsidy_cost, 0) AS NUMERIC(23, 2))
            AS original_loan_subsidy_cost,
        CAST(COALESCE(transaction_normalized.face_value_loan_guarantee, 0) AS NUMERIC(23, 2))
            AS face_value_loan_guarantee,
        transaction_normalized.indirect_federal_sharing,
        transaction_normalized.funding_amount,
        CAST(COALESCE(transaction_fabs.total_funding_amount, '0') AS NUMERIC(23, 2))
            AS total_funding_amount,
        transaction_normalized.non_federal_funding_amount,

        -- Recipient
        COALESCE(
            recipient_lookup.recipient_hash,
            REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        ) AS recipient_hash,
        RECIPIENT_HASH_AND_LEVELS.recipient_levels,
        COALESCE(
            transaction_fpds.awardee_or_recipient_uei,
            transaction_fabs.uei
        ) AS recipient_uei,
        COALESCE(
            transaction_fpds.awardee_or_recipient_legal,
            transaction_fabs.awardee_or_recipient_legal
        ) AS recipient_name_raw,
        UPPER(COALESCE(
            recipient_lookup.legal_business_name,
            transaction_fpds.awardee_or_recipient_legal,
            transaction_fabs.awardee_or_recipient_legal
        )) AS recipient_name,
        COALESCE(
            transaction_fpds.awardee_or_recipient_uniqu,
            transaction_fabs.awardee_or_recipient_uniqu
        ) AS recipient_unique_id,
        PRL.recipient_hash AS parent_recipient_hash,
        COALESCE(
            transaction_fpds.ultimate_parent_uei,
            transaction_fabs.ultimate_parent_uei
        ) AS parent_uei,
        COALESCE(
            transaction_fpds.ultimate_parent_legal_enti,
            transaction_fabs.ultimate_parent_legal_enti
        ) AS parent_recipient_name_raw,
        UPPER(PRL.legal_business_name) AS parent_recipient_name,
        COALESCE(
            transaction_fpds.ultimate_parent_unique_ide,
            transaction_fabs.ultimate_parent_unique_ide
        ) AS parent_recipient_unique_id,

        -- Recipient Location
        COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code)
            AS recipient_location_country_code,
        COALESCE(transaction_fpds.legal_entity_country_name, transaction_fabs.legal_entity_country_name)
            AS recipient_location_country_name,
        COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
            AS recipient_location_state_code,
        COALESCE(transaction_fpds.legal_entity_state_descrip, transaction_fabs.legal_entity_state_name)
            AS recipient_location_state_name,
        RL_STATE_LOOKUP.fips AS recipient_location_state_fips,
        RL_STATE_POPULATION.latest_population AS recipient_location_state_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS recipient_location_county_code,
        COALESCE(transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name)
            AS recipient_location_county_name,
        RL_COUNTY_POPULATION.latest_population AS recipient_location_county_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS recipient_location_congressional_code,
        RL_DISTRICT_POPULATION.latest_population AS recipient_location_congressional_population,
        CURRENT_CD.recipient_location_congressional_code_current AS recipient_location_congressional_code_current,
        COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5)
            AS recipient_location_zip5,
        transaction_fpds.legal_entity_zip4,
        COALESCE(transaction_fpds.legal_entity_zip_last4, transaction_fabs.legal_entity_zip_last4)
            AS legal_entity_zip_last4,
        transaction_fabs.legal_entity_city_code,
        TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name))
            AS recipient_location_city_name,
        COALESCE(transaction_fpds.legal_entity_address_line1, transaction_fabs.legal_entity_address_line1)
            AS legal_entity_address_line1,
        COALESCE(transaction_fpds.legal_entity_address_line2, transaction_fabs.legal_entity_address_line2)
            AS legal_entity_address_line2,
        COALESCE(transaction_fpds.legal_entity_address_line3, transaction_fabs.legal_entity_address_line3)
            AS legal_entity_address_line3,
        transaction_fabs.legal_entity_foreign_city,
        transaction_fabs.legal_entity_foreign_descr,
        transaction_fabs.legal_entity_foreign_posta,
        transaction_fabs.legal_entity_foreign_provi,
        CONCAT(
            RL_STATE_LOOKUP.fips,
            COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code)
        ) AS recipient_location_county_fips,

        -- Place of Performance
        transaction_fabs.place_of_performance_code,
        transaction_fabs.place_of_performance_scope,
        COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c)
            AS pop_country_code,
        COALESCE(transaction_fpds.place_of_perf_country_desc, transaction_fabs.place_of_perform_country_n)
            AS pop_country_name,
        COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
            AS pop_state_code,
        COALESCE(transaction_fpds.place_of_perfor_state_desc, transaction_fabs.place_of_perform_state_nam)
            AS pop_state_name,
        POP_STATE_LOOKUP.fips AS pop_state_fips,
        POP_STATE_POPULATION.latest_population AS pop_state_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS pop_county_code,
        COALESCE(transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na)
            AS pop_county_name,
        POP_COUNTY_POPULATION.latest_population AS pop_county_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS pop_congressional_code,
        POP_DISTRICT_POPULATION.latest_population AS pop_congressional_population,
        CURRENT_CD.pop_congressional_code_current AS pop_congressional_code_current,
        COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5)
            AS pop_zip5,
        COALESCE(transaction_fpds.place_of_performance_zip4a, transaction_fabs.place_of_performance_zip4a)
            AS place_of_performance_zip4a,
        COALESCE(transaction_fpds.place_of_perform_zip_last4, transaction_fabs.place_of_perform_zip_last4)
            AS place_of_perform_zip_last4,
        TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city))
            AS pop_city_name,
        transaction_fabs.place_of_performance_forei AS place_of_performance_forei,
        CONCAT(
            POP_STATE_LOOKUP.fips,
            COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co)
        ) AS pop_county_fips,

        -- Accounts
        FED_AND_TRES_ACCT.treasury_account_identifiers,
        FED_AND_TRES_ACCT.tas_paths,
        FED_AND_TRES_ACCT.tas_components,
        FED_AND_TRES_ACCT.federal_accounts,
        FED_AND_TRES_ACCT.disaster_emergency_fund_codes,

        -- Officer Amounts
        COALESCE(transaction_fabs.officer_1_name, transaction_fpds.officer_1_name) AS officer_1_name,
        COALESCE(transaction_fabs.officer_1_amount, transaction_fpds.officer_1_amount) AS officer_1_amount,
        COALESCE(transaction_fabs.officer_2_name, transaction_fpds.officer_2_name) AS officer_2_name,
        COALESCE(transaction_fabs.officer_2_amount, transaction_fpds.officer_2_amount) AS officer_2_amount,
        COALESCE(transaction_fabs.officer_3_name, transaction_fpds.officer_3_name) AS officer_3_name,
        COALESCE(transaction_fabs.officer_3_amount, transaction_fpds.officer_3_amount) AS officer_3_amount,
        COALESCE(transaction_fabs.officer_4_name, transaction_fpds.officer_4_name) AS officer_4_name,
        COALESCE(transaction_fabs.officer_4_amount, transaction_fpds.officer_4_amount) AS officer_4_amount,
        COALESCE(transaction_fabs.officer_5_name, transaction_fpds.officer_5_name) AS officer_5_name,
        COALESCE(transaction_fabs.officer_5_amount, transaction_fpds.officer_5_amount) AS officer_5_amount,

        -- Exclusively FABS
        transaction_fabs.published_fabs_id,
        transaction_fabs.afa_generated_unique,
        transaction_fabs.business_funds_ind_desc,
        transaction_fabs.business_funds_indicator,
        transaction_fabs.business_types,
        transaction_fabs.business_types_desc,
        transaction_fabs.cfda_number,
        transaction_fabs.cfda_title,
        references_cfda.id AS cfda_id,
        transaction_fabs.correction_delete_indicatr,
        transaction_fabs.correction_delete_ind_desc,
        awards.fain,
        transaction_fabs.funding_opportunity_goals,
        transaction_fabs.funding_opportunity_number,
        transaction_fabs.record_type,
        transaction_fabs.record_type_description,
        transaction_fabs.sai_number,
        awards.uri,

        -- Exclusively FPDS
        transaction_fpds.detached_award_procurement_id,
        transaction_fpds.detached_award_proc_unique,
        transaction_fpds.a_76_fair_act_action,
        transaction_fpds.a_76_fair_act_action_desc,
        transaction_fpds.agency_id,
        transaction_fpds.airport_authority,
        transaction_fpds.alaskan_native_owned_corpo,
        transaction_fpds.alaskan_native_servicing_i,
        transaction_fpds.american_indian_owned_busi,
        transaction_fpds.asian_pacific_american_own,
        transaction_fpds.base_and_all_options_value,
        transaction_fpds.base_exercised_options_val,
        transaction_fpds.black_american_owned_busin,
        transaction_fpds.c1862_land_grant_college,
        transaction_fpds.c1890_land_grant_college,
        transaction_fpds.c1994_land_grant_college,
        transaction_fpds.c8a_program_participant,
        transaction_fpds.cage_code,
        transaction_fpds.city_local_government,
        transaction_fpds.clinger_cohen_act_planning,
        transaction_fpds.clinger_cohen_act_pla_desc,
        transaction_fpds.commercial_item_acqui_desc,
        transaction_fpds.commercial_item_acquisitio,
        transaction_fpds.commercial_item_test_desc,
        transaction_fpds.commercial_item_test_progr,
        transaction_fpds.community_developed_corpor,
        transaction_fpds.community_development_corp,
        transaction_fpds.consolidated_contract,
        transaction_fpds.consolidated_contract_desc,
        transaction_fpds.construction_wage_rat_desc,
        transaction_fpds.construction_wage_rate_req,
        transaction_fpds.contingency_humanitar_desc,
        transaction_fpds.contingency_humanitarian_o,
        transaction_fpds.contract_award_type,
        transaction_fpds.contract_award_type_desc,
        transaction_fpds.contract_bundling,
        transaction_fpds.contract_bundling_descrip,
        transaction_fpds.contract_financing,
        transaction_fpds.contract_financing_descrip,
        transaction_fpds.contracting_officers_desc,
        transaction_fpds.contracting_officers_deter,
        transaction_fpds.contracts,
        transaction_fpds.corporate_entity_not_tax_e,
        transaction_fpds.corporate_entity_tax_exemp,
        transaction_fpds.cost_accounting_stand_desc,
        transaction_fpds.cost_accounting_standards,
        transaction_fpds.cost_or_pricing_data,
        transaction_fpds.cost_or_pricing_data_desc,
        transaction_fpds.council_of_governments,
        transaction_fpds.country_of_product_or_desc,
        transaction_fpds.country_of_product_or_serv,
        transaction_fpds.county_local_government,
        transaction_fpds.current_total_value_award,
        transaction_fpds.dod_claimant_prog_cod_desc,
        transaction_fpds.dod_claimant_program_code,
        transaction_fpds.domestic_or_foreign_e_desc,
        transaction_fpds.domestic_or_foreign_entity,
        transaction_fpds.domestic_shelter,
        transaction_fpds.dot_certified_disadvantage,
        transaction_fpds.economically_disadvantaged,
        transaction_fpds.educational_institution,
        transaction_fpds.emerging_small_business,
        transaction_fpds.epa_designated_produc_desc,
        transaction_fpds.epa_designated_product,
        transaction_fpds.evaluated_preference,
        transaction_fpds.evaluated_preference_desc,
        transaction_fpds.extent_competed,
        transaction_fpds.extent_compete_description,
        transaction_fpds.fair_opportunity_limi_desc,
        transaction_fpds.fair_opportunity_limited_s,
        transaction_fpds.fed_biz_opps,
        transaction_fpds.fed_biz_opps_description,
        transaction_fpds.federal_agency,
        transaction_fpds.federally_funded_research,
        transaction_fpds.for_profit_organization,
        transaction_fpds.foreign_funding,
        transaction_fpds.foreign_funding_desc,
        transaction_fpds.foreign_government,
        transaction_fpds.foreign_owned_and_located,
        transaction_fpds.foundation,
        transaction_fpds.government_furnished_desc,
        transaction_fpds.government_furnished_prope,
        transaction_fpds.grants,
        transaction_fpds.hispanic_american_owned_bu,
        transaction_fpds.hispanic_servicing_institu,
        transaction_fpds.historically_black_college,
        transaction_fpds.historically_underutilized,
        transaction_fpds.hospital_flag,
        transaction_fpds.housing_authorities_public,
        transaction_fpds.idv_type,
        transaction_fpds.idv_type_description,
        transaction_fpds.indian_tribe_federally_rec,
        transaction_fpds.information_technolog_desc,
        transaction_fpds.information_technology_com,
        transaction_fpds.inherently_government_desc,
        transaction_fpds.inherently_government_func,
        transaction_fpds.inter_municipal_local_gove,
        transaction_fpds.interagency_contract_desc,
        transaction_fpds.interagency_contracting_au,
        transaction_fpds.international_organization,
        transaction_fpds.interstate_entity,
        transaction_fpds.joint_venture_economically,
        transaction_fpds.joint_venture_women_owned,
        transaction_fpds.labor_standards,
        transaction_fpds.labor_standards_descrip,
        transaction_fpds.labor_surplus_area_firm,
        transaction_fpds.limited_liability_corporat,
        transaction_fpds.local_area_set_aside,
        transaction_fpds.local_area_set_aside_desc,
        transaction_fpds.local_government_owned,
        transaction_fpds.major_program,
        transaction_fpds.manufacturer_of_goods,
        transaction_fpds.materials_supplies_article,
        transaction_fpds.materials_supplies_descrip,
        transaction_fpds.minority_institution,
        transaction_fpds.minority_owned_business,
        transaction_fpds.multi_year_contract,
        transaction_fpds.multi_year_contract_desc,
        transaction_fpds.multiple_or_single_aw_desc,
        transaction_fpds.multiple_or_single_award_i,
        transaction_fpds.municipality_local_governm,
        transaction_fpds.naics AS naics_code,
        transaction_fpds.naics_description AS naics_description,
        transaction_fpds.national_interest_action,
        transaction_fpds.national_interest_desc,
        transaction_fpds.native_american_owned_busi,
        transaction_fpds.native_hawaiian_owned_busi,
        transaction_fpds.native_hawaiian_servicing,
        transaction_fpds.nonprofit_organization,
        transaction_fpds.number_of_actions,
        transaction_fpds.number_of_offers_received,
        transaction_fpds.ordering_period_end_date,
        transaction_fpds.organizational_type,
        transaction_fpds.other_minority_owned_busin,
        transaction_fpds.other_not_for_profit_organ,
        transaction_fpds.other_statutory_authority,
        transaction_fpds.other_than_full_and_o_desc,
        transaction_fpds.other_than_full_and_open_c,
        transaction_fpds.parent_award_id,
        transaction_fpds.partnership_or_limited_lia,
        transaction_fpds.performance_based_se_desc,
        transaction_fpds.performance_based_service,
        transaction_fpds.period_of_perf_potential_e,
        awards.piid,
        transaction_fpds.place_of_manufacture,
        transaction_fpds.place_of_manufacture_desc,
        transaction_fpds.planning_commission,
        transaction_fpds.port_authority,
        transaction_fpds.potential_total_value_awar,
        transaction_fpds.price_evaluation_adjustmen,
        transaction_fpds.private_university_or_coll,
        transaction_fpds.product_or_service_code,
        transaction_fpds.product_or_service_co_desc AS product_or_service_description,
        transaction_fpds.program_acronym,
        transaction_fpds.program_system_or_equ_desc,
        transaction_fpds.program_system_or_equipmen,
        transaction_fpds.pulled_from,
        transaction_fpds.purchase_card_as_paym_desc,
        transaction_fpds.purchase_card_as_payment_m,
        transaction_fpds.receives_contracts_and_gra,
        transaction_fpds.recovered_materials_s_desc,
        transaction_fpds.recovered_materials_sustai,
        transaction_fpds.referenced_idv_agency_desc,
        transaction_fpds.referenced_idv_agency_iden,
        transaction_fpds.referenced_idv_modificatio,
        transaction_fpds.referenced_idv_type,
        transaction_fpds.referenced_idv_type_desc,
        transaction_fpds.referenced_mult_or_si_desc,
        transaction_fpds.referenced_mult_or_single,
        transaction_fpds.research,
        transaction_fpds.research_description,
        transaction_fpds.sam_exception,
        transaction_fpds.sam_exception_description,
        transaction_fpds.sba_certified_8_a_joint_ve,
        transaction_fpds.school_district_local_gove,
        transaction_fpds.school_of_forestry,
        transaction_fpds.sea_transportation,
        transaction_fpds.sea_transportation_desc,
        transaction_fpds.self_certified_small_disad,
        transaction_fpds.service_disabled_veteran_o,
        transaction_fpds.small_agricultural_coopera,
        transaction_fpds.small_business_competitive,
        transaction_fpds.small_disadvantaged_busine,
        transaction_fpds.sole_proprietorship,
        transaction_fpds.solicitation_date,
        transaction_fpds.solicitation_identifier,
        transaction_fpds.solicitation_procedur_desc,
        transaction_fpds.solicitation_procedures,
        transaction_fpds.state_controlled_instituti,
        transaction_fpds.subchapter_s_corporation,
        transaction_fpds.subcontinent_asian_asian_i,
        transaction_fpds.subcontracting_plan,
        transaction_fpds.subcontracting_plan_desc,
        transaction_fpds.the_ability_one_program,
        transaction_fpds.total_obligated_amount,
        transaction_fpds.township_local_government,
        transaction_fpds.transaction_number,
        transaction_fpds.transit_authority,
        transaction_fpds.tribal_college,
        transaction_fpds.tribally_owned_business,
        transaction_fpds.type_of_contract_pricing,
        transaction_fpds.type_of_contract_pric_desc,
        transaction_fpds.type_of_idc,
        transaction_fpds.type_of_idc_description,
        transaction_fpds.type_set_aside,
        transaction_fpds.type_set_aside_description,
        transaction_fpds.undefinitized_action,
        transaction_fpds.undefinitized_action_desc,
        transaction_fpds.us_federal_government,
        transaction_fpds.us_government_entity,
        transaction_fpds.us_local_government,
        transaction_fpds.us_state_government,
        transaction_fpds.us_tribal_government,
        transaction_fpds.vendor_doing_as_business_n,
        transaction_fpds.vendor_fax_number,
        transaction_fpds.vendor_phone_number,
        transaction_fpds.veteran_owned_business,
        transaction_fpds.veterinary_college,
        transaction_fpds.veterinary_hospital,
        transaction_fpds.woman_owned_business,
        transaction_fpds.women_owned_small_business,
        CAST(FED_AND_TRES_ACCT.program_activities AS STRING)

    FROM
        int.transaction_normalized
    LEFT OUTER JOIN
        int.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
    LEFT OUTER JOIN
        int.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
    LEFT OUTER JOIN
        global_temp.references_cfda ON (transaction_fabs.cfda_number = references_cfda.program_number)
    LEFT OUTER JOIN
        rpt.recipient_lookup ON (
            recipient_lookup.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        )
    LEFT OUTER JOIN
        int.awards ON (transaction_normalized.award_id = awards.id)
    LEFT OUTER JOIN
        global_temp.agency AS AA ON (transaction_normalized.awarding_agency_id = AA.id)
    LEFT OUTER JOIN
        global_temp.toptier_agency AS TAA ON (AA.toptier_agency_id = TAA.toptier_agency_id)
    LEFT OUTER JOIN
        global_temp.subtier_agency AS SAA ON (AA.subtier_agency_id = SAA.subtier_agency_id)
    LEFT OUTER JOIN
        global_temp.agency AS AA_ID ON (AA_ID.toptier_agency_id = TAA.toptier_agency_id AND AA_ID.toptier_flag = TRUE)
    LEFT OUTER JOIN
        global_temp.agency AS FA ON (transaction_normalized.funding_agency_id = FA.id)
    LEFT OUTER JOIN
        global_temp.toptier_agency AS TFA ON (FA.toptier_agency_id = TFA.toptier_agency_id)
    LEFT OUTER JOIN
        global_temp.subtier_agency AS SFA ON (FA.subtier_agency_id = SFA.subtier_agency_id)
   LEFT OUTER JOIN
        (SELECT id, toptier_agency_id, ROW_NUMBER() OVER (PARTITION BY toptier_agency_id ORDER BY toptier_flag DESC, id ASC) AS row_num FROM global_temp.agency) AS FA_ID
        ON (FA_ID.toptier_agency_id = TFA.toptier_agency_id AND row_num = 1)
    LEFT OUTER JOIN
        rpt.recipient_lookup PRL ON (
            PRL.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei))
                    WHEN COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.ultimate_parent_legal_enti, transaction_fabs.ultimate_parent_legal_enti, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        )
    LEFT OUTER JOIN (
        SELECT recipient_hash, uei, SORT_ARRAY(COLLECT_SET(recipient_level)) AS recipient_levels
        FROM rpt.recipient_profile
        GROUP BY recipient_hash, uei
    ) RECIPIENT_HASH_AND_LEVELS ON (
        recipient_lookup.recipient_hash = RECIPIENT_HASH_AND_LEVELS.recipient_hash
        AND recipient_lookup.legal_business_name NOT IN (
            'MULTIPLE RECIPIENTS',
            'REDACTED DUE TO PII',
            'MULTIPLE FOREIGN RECIPIENTS',
            'PRIVATE INDIVIDUAL',
            'INDIVIDUAL RECIPIENT',
            'MISCELLANEOUS FOREIGN AWARDEES'
        )
        AND recipient_lookup.legal_business_name IS NOT NULL
    )
    LEFT OUTER JOIN (
        SELECT code, name, fips, MAX(id)
        FROM global_temp.state_data
        GROUP BY code, name, fips
    ) POP_STATE_LOOKUP ON (
        POP_STATE_LOOKUP.code = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
    )
    LEFT OUTER JOIN
        global_temp.ref_population_county POP_STATE_POPULATION ON (
            POP_STATE_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_STATE_POPULATION.county_number = '000'
        )
    LEFT OUTER JOIN
        global_temp.ref_population_county POP_COUNTY_POPULATION ON (
            POP_COUNTY_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
        )
    LEFT OUTER JOIN
        global_temp.ref_population_cong_district POP_DISTRICT_POPULATION ON (
            POP_DISTRICT_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
        )
    LEFT OUTER JOIN (
        SELECT code, name, fips, MAX(id)
        FROM global_temp.state_data
        GROUP BY code, name, fips
    ) RL_STATE_LOOKUP ON (
        RL_STATE_LOOKUP.code = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
    )
    LEFT OUTER JOIN
        global_temp.ref_population_county RL_STATE_POPULATION ON (
            RL_STATE_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_STATE_POPULATION.county_number = '000'
        )
    LEFT OUTER JOIN
        global_temp.ref_population_county RL_COUNTY_POPULATION ON (
            RL_COUNTY_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
        )
    LEFT OUTER JOIN
        global_temp.ref_population_cong_district RL_DISTRICT_POPULATION ON (
            RL_DISTRICT_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
        )
    LEFT OUTER JOIN
        int.transaction_current_cd_lookup AS CURRENT_CD ON (
            transaction_normalized.id = CURRENT_CD.transaction_id
        )
    LEFT OUTER JOIN
        global_temp.office awarding_office ON (awarding_office.office_code = COALESCE(transaction_fabs.awarding_office_code, transaction_fpds.awarding_office_code))
    LEFT OUTER JOIN
        global_temp.office funding_office ON (funding_office.office_code = COALESCE(transaction_fabs.funding_office_code, transaction_fpds.funding_office_code))

    LEFT OUTER JOIN (
        SELECT
            faba.award_id,
            TO_JSON(
                SORT_ARRAY(
                    COLLECT_SET(
                        NAMED_STRUCT(
                            'id', fa.id,
                            'account_title', fa.account_title,
                            'federal_account_code', fa.federal_account_code
                        )
                    )
                )
            ) AS federal_accounts,
            -- "CASE" put in place so that Spark value matches Postgres; can most likely be refactored out in the future
            CASE
                WHEN SIZE(COLLECT_SET(faba.disaster_emergency_fund_code)) > 0
                    THEN SORT_ARRAY(COLLECT_SET(faba.disaster_emergency_fund_code))
                ELSE NULL
            END AS disaster_emergency_fund_codes,
            SORT_ARRAY(COLLECT_SET(taa.treasury_account_identifier)) treasury_account_identifiers,
            SORT_ARRAY(
                COLLECT_SET(
                    CONCAT(
                        'agency=', COALESCE(agency.toptier_code, ''),
                        'faaid=', COALESCE(fa.agency_identifier, ''),
                        'famain=', COALESCE(fa.main_account_code, ''),
                        'aid=', COALESCE(taa.agency_id, ''),
                        'main=', COALESCE(taa.main_account_code, ''),
                        'ata=', COALESCE(taa.allocation_transfer_agency_id, ''),
                        'sub=', COALESCE(taa.sub_account_code, ''),
                        'bpoa=', COALESCE(taa.beginning_period_of_availability, ''),
                        'epoa=', COALESCE(taa.ending_period_of_availability, ''),
                        'a=', COALESCE(taa.availability_type_code, '')
                    )
                )
            ) AS tas_paths,
            SORT_ARRAY(
                COLLECT_SET(
                    CONCAT(
                        'aid=', COALESCE(taa.agency_id, ''),
                        'main=', COALESCE(taa.main_account_code, ''),
                        'ata=', COALESCE(taa.allocation_transfer_agency_id, ''),
                        'sub=', COALESCE(taa.sub_account_code, ''),
                        'bpoa=', COALESCE(taa.beginning_period_of_availability, ''),
                        'epoa=', COALESCE(taa.ending_period_of_availability, ''),
                        'a=', COALESCE(taa.availability_type_code, '')
                    )
                ),
                TRUE
            ) AS tas_components,
            COLLECT_SET(
                TO_JSON(
                    NAMED_STRUCT(
                        'name', UPPER(rpa.program_activity_name),
                        'code', LPAD(rpa.program_activity_code, 4, "0")
                    )
                )
            ) AS program_activities
        FROM int.financial_accounts_by_awards AS faba
        INNER JOIN global_temp.treasury_appropriation_account AS taa ON taa.treasury_account_identifier = faba.treasury_account_id
        INNER JOIN global_temp.federal_account AS fa ON fa.id = taa.federal_account_id
        INNER JOIN global_temp.toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
        LEFT JOIN global_temp.ref_program_activity rpa ON (faba.program_activity_id = rpa.id)
        WHERE faba.award_id IS NOT NULL
        GROUP BY faba.award_id
    ) FED_AND_TRES_ACCT ON (FED_AND_TRES_ACCT.award_id = transaction_normalized.award_id)
"""

transaction_search_incremental_load_sql_string = [
    f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_transaction_search_view AS (
        {_base_load_sql_string}
    )
    """,
    f"""
    MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} AS t
    USING (SELECT * FROM temp_transaction_search_view) AS s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND
      ({" OR ".join([f"NOT (s.{col} <=> t.{col})" for col in TRANSACTION_SEARCH_GOLD_DELTA_COLUMNS])})
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """,
]

transaction_search_overwrite_load_sql_string = f"""
INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in TRANSACTION_SEARCH_GOLD_DELTA_COLUMNS])}
    )
    {_base_load_sql_string}
"""
