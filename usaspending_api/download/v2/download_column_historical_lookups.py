"""
Sets up mappings from column names used in downloads to the query paths used to get the data from django.

Not in use while we pull CSV data from the non-historical tables. Until we switch to pulling CSV downloads from the
historical tables TransactionFPDS and TransactionFABS, import download_column_lookups.py instead.

NOTE: To allow for annotations to be used on download a pair of ("<alias>", None) is used so that a placeholder
for the column is made, but it can be removed to avoid being used as a query path.

Code to generate these from spreadsheets:

tail -n +3 'usaspending_api/data/DAIMS_IDD_Resorted+DRW+KB+GGv7/D2-Award (Financial Assistance)-Table 1.csv' >
d2_columns.csv
"""
import copy
from collections import OrderedDict
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR


query_paths = {
    "award": {
        "d1": OrderedDict(
            [
                ("contract_award_unique_key", "generated_unique_award_id"),
                ("award_id_piid", "piid"),
                ("parent_award_agency_id", "latest_transaction__contract_data__referenced_idv_agency_iden"),
                ("parent_award_agency_name", "latest_transaction__contract_data__referenced_idv_agency_desc"),
                ("parent_award_id_piid", "parent_award_piid"),
                (
                    "disaster_emergency_fund_codes" + NAMING_CONFLICT_DISCRIMINATOR,
                    None,
                ),  # Annotation is used to create this column
                ("outlayed_amount_funded_by_COVID-19_supplementals", None),  # Annotation is used to create this column
                ("obligated_amount_funded_by_COVID-19_supplementals", None),  # Annotation is used to create this column
                ("total_obligated_amount", "total_obligation"),
                ("current_total_value_of_award", "latest_transaction__contract_data__current_total_value_award"),
                (
                    "potential_total_value_of_award",
                    "latest_transaction__contract_data__potential_total_value_awar",
                ),
                ("award_base_action_date", "date_signed"),
                ("award_base_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("award_latest_action_date", "latest_transaction__action_date"),
                ("award_latest_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("period_of_performance_start_date", "period_of_performance_start_date"),
                (
                    "period_of_performance_current_end_date",
                    "latest_transaction__contract_data__period_of_performance_curr",
                ),
                (
                    "period_of_performance_potential_end_date",
                    "latest_transaction__contract_data__period_of_perf_potential_e",
                ),
                ("ordering_period_end_date", "latest_transaction__contract_data__ordering_period_end_date"),
                ("solicitation_date", "earliest_transaction__contract_data__solicitation_date"),
                ("awarding_agency_code", "latest_transaction__contract_data__awarding_agency_code"),
                ("awarding_agency_name", "latest_transaction__contract_data__awarding_agency_name"),
                ("awarding_sub_agency_code", "latest_transaction__contract_data__awarding_sub_tier_agency_c"),
                ("awarding_sub_agency_name", "latest_transaction__contract_data__awarding_sub_tier_agency_n"),
                ("awarding_office_code", "latest_transaction__contract_data__awarding_office_code"),
                ("awarding_office_name", "latest_transaction__contract_data__awarding_office_name"),
                ("funding_agency_code", "latest_transaction__contract_data__funding_agency_code"),
                ("funding_agency_name", "latest_transaction__contract_data__funding_agency_name"),
                ("funding_sub_agency_code", "latest_transaction__contract_data__funding_sub_tier_agency_co"),
                ("funding_sub_agency_name", "latest_transaction__contract_data__funding_sub_tier_agency_na"),
                ("funding_office_code", "latest_transaction__contract_data__funding_office_code"),
                ("funding_office_name", "latest_transaction__contract_data__funding_office_name"),
                ("treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("foreign_funding", "latest_transaction__contract_data__foreign_funding"),
                ("foreign_funding_description", "latest_transaction__contract_data__foreign_funding_desc"),
                ("sam_exception", "latest_transaction__contract_data__sam_exception"),
                ("sam_exception_description", "latest_transaction__contract_data__sam_exception_description"),
                ("recipient_uei", "latest_transaction__contract_data__awardee_or_recipient_uei"),
                ("recipient_duns", "latest_transaction__contract_data__awardee_or_recipient_uniqu"),
                ("recipient_name", "latest_transaction__contract_data__awardee_or_recipient_legal"),
                (
                    "recipient_doing_business_as_name",
                    "latest_transaction__contract_data__vendor_doing_as_business_n",
                ),
                ("cage_code", "latest_transaction__contract_data__cage_code"),
                ("recipient_parent_uei", "latest_transaction__contract_data__ultimate_parent_uei"),
                ("recipient_parent_duns", "latest_transaction__contract_data__ultimate_parent_unique_ide"),
                ("recipient_parent_name", "latest_transaction__contract_data__ultimate_parent_legal_enti"),
                ("recipient_country_code", "latest_transaction__contract_data__legal_entity_country_code"),
                ("recipient_country_name", "latest_transaction__contract_data__legal_entity_country_name"),
                ("recipient_address_line_1", "latest_transaction__contract_data__legal_entity_address_line1"),
                ("recipient_address_line_2", "latest_transaction__contract_data__legal_entity_address_line2"),
                ("recipient_city_name", "latest_transaction__contract_data__legal_entity_city_name"),
                ("recipient_county_name", "latest_transaction__contract_data__legal_entity_county_name"),
                ("recipient_state_code", "latest_transaction__contract_data__legal_entity_state_code"),
                ("recipient_state_name", "latest_transaction__contract_data__legal_entity_state_descrip"),
                ("recipient_zip_4_code", "latest_transaction__contract_data__legal_entity_zip4"),
                (
                    "recipient_congressional_district",
                    "latest_transaction__contract_data__legal_entity_congressional",
                ),
                ("recipient_phone_number", "latest_transaction__contract_data__vendor_phone_number"),
                ("recipient_fax_number", "latest_transaction__contract_data__vendor_fax_number"),
                (
                    "primary_place_of_performance_country_code",
                    "latest_transaction__contract_data__place_of_perform_country_c",
                ),
                (
                    "primary_place_of_performance_country_name",
                    "latest_transaction__contract_data__place_of_perf_country_desc",
                ),
                (
                    "primary_place_of_performance_city_name",
                    "latest_transaction__contract_data__place_of_perform_city_name",
                ),
                (
                    "primary_place_of_performance_county_name",
                    "latest_transaction__contract_data__place_of_perform_county_na",
                ),
                (
                    "primary_place_of_performance_state_code",
                    "latest_transaction__contract_data__place_of_performance_state",
                ),
                (
                    "primary_place_of_performance_state_name",
                    "latest_transaction__contract_data__place_of_perfor_state_desc",
                ),
                (
                    "primary_place_of_performance_zip_4",
                    "latest_transaction__contract_data__place_of_performance_zip4a",
                ),
                (
                    "primary_place_of_performance_congressional_district",
                    "latest_transaction__contract_data__place_of_performance_congr",
                ),
                ("award_or_idv_flag", "latest_transaction__contract_data__pulled_from"),
                (
                    "award_type_code",
                    "latest_transaction__contract_data__contract_award_type",
                ),  # Column is appended to in account_download.py
                (
                    "award_type",
                    "latest_transaction__contract_data__contract_award_type_desc",
                ),  # Column is appended to in account_download.py
                ("idv_type_code", "latest_transaction__contract_data__idv_type"),
                ("idv_type", "latest_transaction__contract_data__idv_type_description"),
                (
                    "multiple_or_single_award_idv_code",
                    "latest_transaction__contract_data__multiple_or_single_award_i",
                ),
                (
                    "multiple_or_single_award_idv",
                    "latest_transaction__contract_data__multiple_or_single_aw_desc",
                ),
                ("type_of_idc_code", "latest_transaction__contract_data__type_of_idc"),
                ("type_of_idc", "latest_transaction__contract_data__type_of_idc_description"),
                ("type_of_contract_pricing_code", "latest_transaction__contract_data__type_of_contract_pricing"),
                ("type_of_contract_pricing", "latest_transaction__contract_data__type_of_contract_pric_desc"),
                ("prime_award_base_transaction_description", "description"),
                ("solicitation_identifier", "latest_transaction__contract_data__solicitation_identifier"),
                ("number_of_actions", "latest_transaction__contract_data__number_of_actions"),
                (
                    "inherently_governmental_functions",
                    "latest_transaction__contract_data__inherently_government_func",
                ),
                (
                    "inherently_governmental_functions_description",
                    "latest_transaction__contract_data__inherently_government_desc",
                ),
                ("product_or_service_code", "latest_transaction__contract_data__product_or_service_code"),
                (
                    "product_or_service_code_description",
                    "latest_transaction__contract_data__product_or_service_co_desc",
                ),
                ("contract_bundling_code", "latest_transaction__contract_data__contract_bundling"),
                ("contract_bundling", "latest_transaction__contract_data__contract_bundling_descrip"),
                ("dod_claimant_program_code", "latest_transaction__contract_data__dod_claimant_program_code"),
                (
                    "dod_claimant_program_description",
                    "latest_transaction__contract_data__dod_claimant_prog_cod_desc",
                ),
                ("naics_code", "latest_transaction__contract_data__naics"),
                ("naics_description", "latest_transaction__contract_data__naics_description"),
                (
                    "recovered_materials_sustainability_code",
                    "latest_transaction__contract_data__recovered_materials_sustai",
                ),
                (
                    "recovered_materials_sustainability",
                    "latest_transaction__contract_data__recovered_materials_s_desc",
                ),
                (
                    "domestic_or_foreign_entity_code",
                    "latest_transaction__contract_data__domestic_or_foreign_entity",
                ),
                ("domestic_or_foreign_entity", "latest_transaction__contract_data__domestic_or_foreign_e_desc"),
                (
                    "dod_acquisition_program_code",
                    "latest_transaction__contract_data__program_system_or_equipmen",
                ),
                (
                    "dod_acquisition_program_description",
                    "latest_transaction__contract_data__program_system_or_equ_desc",
                ),
                (
                    "information_technology_commercial_item_category_code",
                    "latest_transaction__contract_data__information_technology_com",
                ),
                (
                    "information_technology_commercial_item_category",
                    "latest_transaction__contract_data__information_technolog_desc",
                ),
                ("epa_designated_product_code", "latest_transaction__contract_data__epa_designated_product"),
                ("epa_designated_product", "latest_transaction__contract_data__epa_designated_produc_desc"),
                (
                    "country_of_product_or_service_origin_code",
                    "latest_transaction__contract_data__country_of_product_or_serv",
                ),
                (
                    "country_of_product_or_service_origin",
                    "latest_transaction__contract_data__country_of_product_or_desc",
                ),
                ("place_of_manufacture_code", "latest_transaction__contract_data__place_of_manufacture"),
                ("place_of_manufacture", "latest_transaction__contract_data__place_of_manufacture_desc"),
                ("subcontracting_plan_code", "latest_transaction__contract_data__subcontracting_plan"),
                ("subcontracting_plan", "latest_transaction__contract_data__subcontracting_plan_desc"),
                ("extent_competed_code", "latest_transaction__contract_data__extent_competed"),
                ("extent_competed", "latest_transaction__contract_data__extent_compete_description"),
                ("solicitation_procedures_code", "latest_transaction__contract_data__solicitation_procedures"),
                ("solicitation_procedures", "latest_transaction__contract_data__solicitation_procedur_desc"),
                ("type_of_set_aside_code", "latest_transaction__contract_data__type_set_aside"),
                ("type_of_set_aside", "latest_transaction__contract_data__type_set_aside_description"),
                ("evaluated_preference_code", "latest_transaction__contract_data__evaluated_preference"),
                ("evaluated_preference", "latest_transaction__contract_data__evaluated_preference_desc"),
                ("research_code", "latest_transaction__contract_data__research"),
                ("research", "latest_transaction__contract_data__research_description"),
                (
                    "fair_opportunity_limited_sources_code",
                    "latest_transaction__contract_data__fair_opportunity_limited_s",
                ),
                (
                    "fair_opportunity_limited_sources",
                    "latest_transaction__contract_data__fair_opportunity_limi_desc",
                ),
                (
                    "other_than_full_and_open_competition_code",
                    "latest_transaction__contract_data__other_than_full_and_open_c",
                ),
                (
                    "other_than_full_and_open_competition",
                    "latest_transaction__contract_data__other_than_full_and_o_desc",
                ),
                ("number_of_offers_received", "latest_transaction__contract_data__number_of_offers_received"),
                (
                    "commercial_item_acquisition_procedures_code",
                    "latest_transaction__contract_data__commercial_item_acquisitio",
                ),
                (
                    "commercial_item_acquisition_procedures",
                    "latest_transaction__contract_data__commercial_item_acqui_desc",
                ),
                (
                    "small_business_competitiveness_demonstration_program",
                    "latest_transaction__contract_data__small_business_competitive",
                ),
                (
                    "simplified_procedures_for_certain_commercial_items_code",
                    "latest_transaction__contract_data__commercial_item_test_progr",
                ),
                (
                    "simplified_procedures_for_certain_commercial_items",
                    "latest_transaction__contract_data__commercial_item_test_desc",
                ),
                ("a76_fair_act_action_code", "latest_transaction__contract_data__a_76_fair_act_action"),
                ("a76_fair_act_action", "latest_transaction__contract_data__a_76_fair_act_action_desc"),
                ("fed_biz_opps_code", "latest_transaction__contract_data__fed_biz_opps"),
                ("fed_biz_opps", "latest_transaction__contract_data__fed_biz_opps_description"),
                ("local_area_set_aside_code", "latest_transaction__contract_data__local_area_set_aside"),
                ("local_area_set_aside", "latest_transaction__contract_data__local_area_set_aside_desc"),
                (
                    "price_evaluation_adjustment_preference_percent_difference",
                    "latest_transaction__contract_data__price_evaluation_adjustmen",
                ),
                (
                    "clinger_cohen_act_planning_code",
                    "latest_transaction__contract_data__clinger_cohen_act_planning",
                ),
                ("clinger_cohen_act_planning", "latest_transaction__contract_data__clinger_cohen_act_pla_desc"),
                (
                    "materials_supplies_articles_equipment_code",
                    "latest_transaction__contract_data__materials_supplies_article",
                ),
                (
                    "materials_supplies_articles_equipment",
                    "latest_transaction__contract_data__materials_supplies_descrip",
                ),
                ("labor_standards_code", "latest_transaction__contract_data__labor_standards"),
                ("labor_standards", "latest_transaction__contract_data__labor_standards_descrip"),
                (
                    "construction_wage_rate_requirements_code",
                    "latest_transaction__contract_data__construction_wage_rate_req",
                ),
                (
                    "construction_wage_rate_requirements",
                    "latest_transaction__contract_data__construction_wage_rat_desc",
                ),
                (
                    "interagency_contracting_authority_code",
                    "latest_transaction__contract_data__interagency_contracting_au",
                ),
                (
                    "interagency_contracting_authority",
                    "latest_transaction__contract_data__interagency_contract_desc",
                ),
                ("other_statutory_authority", "latest_transaction__contract_data__other_statutory_authority"),
                ("program_acronym", "latest_transaction__contract_data__program_acronym"),
                (
                    "parent_award_type_code",
                    "latest_transaction__contract_data__referenced_idv_type",
                ),
                (
                    "parent_award_type",
                    "latest_transaction__contract_data__referenced_idv_type_desc",
                ),
                (
                    "parent_award_single_or_multiple_code",
                    "latest_transaction__contract_data__referenced_mult_or_single",
                ),
                (
                    "parent_award_single_or_multiple",
                    "latest_transaction__contract_data__referenced_mult_or_si_desc",
                ),
                ("major_program", "latest_transaction__contract_data__major_program"),
                ("national_interest_action_code", "latest_transaction__contract_data__national_interest_action"),
                ("national_interest_action", "latest_transaction__contract_data__national_interest_desc"),
                ("cost_or_pricing_data_code", "latest_transaction__contract_data__cost_or_pricing_data"),
                ("cost_or_pricing_data", "latest_transaction__contract_data__cost_or_pricing_data_desc"),
                (
                    "cost_accounting_standards_clause_code",
                    "latest_transaction__contract_data__cost_accounting_standards",
                ),
                (
                    "cost_accounting_standards_clause",
                    "latest_transaction__contract_data__cost_accounting_stand_desc",
                ),
                (
                    "government_furnished_property_code",
                    "latest_transaction__contract_data__government_furnished_prope",
                ),
                (
                    "government_furnished_property",
                    "latest_transaction__contract_data__government_furnished_prope",
                ),
                ("sea_transportation_code", "latest_transaction__contract_data__sea_transportation"),
                ("sea_transportation", "latest_transaction__contract_data__sea_transportation_desc"),
                ("consolidated_contract_code", "latest_transaction__contract_data__consolidated_contract"),
                ("consolidated_contract", "latest_transaction__contract_data__consolidated_contract_desc"),
                (
                    "performance_based_service_acquisition_code",
                    "latest_transaction__contract_data__performance_based_service",
                ),
                (
                    "performance_based_service_acquisition",
                    "latest_transaction__contract_data__performance_based_se_desc",
                ),
                ("multi_year_contract_code", "latest_transaction__contract_data__multi_year_contract"),
                ("multi_year_contract", "latest_transaction__contract_data__multi_year_contract_desc"),
                ("contract_financing_code", "latest_transaction__contract_data__contract_financing"),
                ("contract_financing", "latest_transaction__contract_data__contract_financing_descrip"),
                (
                    "purchase_card_as_payment_method_code",
                    "latest_transaction__contract_data__purchase_card_as_payment_m",
                ),
                (
                    "purchase_card_as_payment_method",
                    "latest_transaction__contract_data__purchase_card_as_paym_desc",
                ),
                (
                    "contingency_humanitarian_or_peacekeeping_operation_code",
                    "latest_transaction__contract_data__contingency_humanitarian_o",
                ),
                (
                    "contingency_humanitarian_or_peacekeeping_operation",
                    "latest_transaction__contract_data__contingency_humanitar_desc",
                ),
                (
                    "alaskan_native_corporation_owned_firm",
                    "latest_transaction__contract_data__alaskan_native_owned_corpo",
                ),
                (
                    "american_indian_owned_business",
                    "latest_transaction__contract_data__american_indian_owned_busi",
                ),
                (
                    "indian_tribe_federally_recognized",
                    "latest_transaction__contract_data__indian_tribe_federally_rec",
                ),
                (
                    "native_hawaiian_organization_owned_firm",
                    "latest_transaction__contract_data__native_hawaiian_owned_busi",
                ),
                ("tribally_owned_firm", "latest_transaction__contract_data__tribally_owned_business"),
                ("veteran_owned_business", "latest_transaction__contract_data__veteran_owned_business"),
                (
                    "service_disabled_veteran_owned_business",
                    "latest_transaction__contract_data__service_disabled_veteran_o",
                ),
                ("woman_owned_business", "latest_transaction__contract_data__woman_owned_business"),
                ("women_owned_small_business", "latest_transaction__contract_data__women_owned_small_business"),
                (
                    "economically_disadvantaged_women_owned_small_business",
                    "latest_transaction__contract_data__economically_disadvantaged",
                ),
                (
                    "joint_venture_women_owned_small_business",
                    "latest_transaction__contract_data__joint_venture_women_owned",
                ),
                (
                    "joint_venture_economic_disadvantaged_women_owned_small_bus",
                    "latest_transaction__contract_data__joint_venture_economically",
                ),
                ("minority_owned_business", "latest_transaction__contract_data__minority_owned_business"),
                (
                    "subcontinent_asian_asian_indian_american_owned_business",
                    "latest_transaction__contract_data__subcontinent_asian_asian_i",
                ),
                (
                    "asian_pacific_american_owned_business",
                    "latest_transaction__contract_data__asian_pacific_american_own",
                ),
                (
                    "black_american_owned_business",
                    "latest_transaction__contract_data__black_american_owned_busin",
                ),
                (
                    "hispanic_american_owned_business",
                    "latest_transaction__contract_data__hispanic_american_owned_bu",
                ),
                (
                    "native_american_owned_business",
                    "latest_transaction__contract_data__native_american_owned_busi",
                ),
                (
                    "other_minority_owned_business",
                    "latest_transaction__contract_data__other_minority_owned_busin",
                ),
                (
                    "contracting_officers_determination_of_business_size",
                    "latest_transaction__contract_data__contracting_officers_desc",
                ),
                (
                    "contracting_officers_determination_of_business_size_code",
                    "latest_transaction__contract_data__contracting_officers_deter",
                ),
                ("emerging_small_business", "latest_transaction__contract_data__emerging_small_business"),
                (
                    "community_developed_corporation_owned_firm",
                    "latest_transaction__contract_data__community_developed_corpor",
                ),
                ("labor_surplus_area_firm", "latest_transaction__contract_data__labor_surplus_area_firm"),
                ("us_federal_government", "latest_transaction__contract_data__us_federal_government"),
                (
                    "federally_funded_research_and_development_corp",
                    "latest_transaction__contract_data__federally_funded_research",
                ),
                ("federal_agency", "latest_transaction__contract_data__federal_agency"),
                ("us_state_government", "latest_transaction__contract_data__us_state_government"),
                ("us_local_government", "latest_transaction__contract_data__us_local_government"),
                ("city_local_government", "latest_transaction__contract_data__city_local_government"),
                ("county_local_government", "latest_transaction__contract_data__county_local_government"),
                (
                    "inter_municipal_local_government",
                    "latest_transaction__contract_data__inter_municipal_local_gove",
                ),
                ("local_government_owned", "latest_transaction__contract_data__local_government_owned"),
                (
                    "municipality_local_government",
                    "latest_transaction__contract_data__municipality_local_governm",
                ),
                (
                    "school_district_local_government",
                    "latest_transaction__contract_data__school_district_local_gove",
                ),
                ("township_local_government", "latest_transaction__contract_data__township_local_government"),
                ("us_tribal_government", "latest_transaction__contract_data__us_tribal_government"),
                ("foreign_government", "latest_transaction__contract_data__foreign_government"),
                ("organizational_type", "latest_transaction__contract_data__organizational_type"),
                (
                    "corporate_entity_not_tax_exempt",
                    "latest_transaction__contract_data__corporate_entity_not_tax_e",
                ),
                ("corporate_entity_tax_exempt", "latest_transaction__contract_data__corporate_entity_tax_exemp"),
                (
                    "partnership_or_limited_liability_partnership",
                    "latest_transaction__contract_data__partnership_or_limited_lia",
                ),
                ("sole_proprietorship", "latest_transaction__contract_data__sole_proprietorship"),
                (
                    "small_agricultural_cooperative",
                    "latest_transaction__contract_data__small_agricultural_coopera",
                ),
                ("international_organization", "latest_transaction__contract_data__international_organization"),
                ("us_government_entity", "latest_transaction__contract_data__us_government_entity"),
                (
                    "community_development_corporation",
                    "latest_transaction__contract_data__community_development_corp",
                ),
                ("domestic_shelter", "latest_transaction__contract_data__domestic_shelter"),
                ("educational_institution", "latest_transaction__contract_data__educational_institution"),
                ("foundation", "latest_transaction__contract_data__foundation"),
                ("hospital_flag", "latest_transaction__contract_data__hospital_flag"),
                ("manufacturer_of_goods", "latest_transaction__contract_data__manufacturer_of_goods"),
                ("veterinary_hospital", "latest_transaction__contract_data__veterinary_hospital"),
                (
                    "hispanic_servicing_institution",
                    "latest_transaction__contract_data__hispanic_servicing_institu",
                ),
                ("receives_contracts", "latest_transaction__contract_data__contracts"),
                ("receives_financial_assistance", "latest_transaction__contract_data__grants"),
                (
                    "receives_contracts_and_financial_assistance",
                    "latest_transaction__contract_data__receives_contracts_and_gra",
                ),
                ("airport_authority", "latest_transaction__contract_data__airport_authority"),
                ("council_of_governments", "latest_transaction__contract_data__council_of_governments"),
                (
                    "housing_authorities_public_tribal",
                    "latest_transaction__contract_data__housing_authorities_public",
                ),
                ("interstate_entity", "latest_transaction__contract_data__interstate_entity"),
                ("planning_commission", "latest_transaction__contract_data__planning_commission"),
                ("port_authority", "latest_transaction__contract_data__port_authority"),
                ("transit_authority", "latest_transaction__contract_data__transit_authority"),
                ("subchapter_scorporation", "latest_transaction__contract_data__subchapter_s_corporation"),
                (
                    "limited_liability_corporation",
                    "latest_transaction__contract_data__limited_liability_corporat",
                ),
                ("foreign_owned", "latest_transaction__contract_data__foreign_owned_and_located"),
                ("for_profit_organization", "latest_transaction__contract_data__for_profit_organization"),
                ("nonprofit_organization", "latest_transaction__contract_data__nonprofit_organization"),
                (
                    "other_not_for_profit_organization",
                    "latest_transaction__contract_data__other_not_for_profit_organ",
                ),
                ("the_ability_one_program", "latest_transaction__contract_data__the_ability_one_program"),
                (
                    "private_university_or_college",
                    "latest_transaction__contract_data__private_university_or_coll",
                ),
                (
                    "state_controlled_institution_of_higher_learning",
                    "latest_transaction__contract_data__state_controlled_instituti",
                ),
                ("1862_land_grant_college", "latest_transaction__contract_data__c1862_land_grant_college"),
                ("1890_land_grant_college", "latest_transaction__contract_data__c1890_land_grant_college"),
                ("1994_land_grant_college", "latest_transaction__contract_data__c1994_land_grant_college"),
                ("minority_institution", "latest_transaction__contract_data__minority_institution"),
                ("historically_black_college", "latest_transaction__contract_data__historically_black_college"),
                ("tribal_college", "latest_transaction__contract_data__tribal_college"),
                (
                    "alaskan_native_servicing_institution",
                    "latest_transaction__contract_data__alaskan_native_servicing_i",
                ),
                (
                    "native_hawaiian_servicing_institution",
                    "latest_transaction__contract_data__native_hawaiian_servicing",
                ),
                ("school_of_forestry", "latest_transaction__contract_data__school_of_forestry"),
                ("veterinary_college", "latest_transaction__contract_data__veterinary_college"),
                ("dot_certified_disadvantage", "latest_transaction__contract_data__dot_certified_disadvantage"),
                (
                    "self_certified_small_disadvantaged_business",
                    "latest_transaction__contract_data__self_certified_small_disad",
                ),
                (
                    "small_disadvantaged_business",
                    "latest_transaction__contract_data__small_disadvantaged_busine",
                ),
                ("c8a_program_participant", "latest_transaction__contract_data__c8a_program_participant"),
                (
                    "historically_underutilized_business_zone_hubzone_firm",
                    "latest_transaction__contract_data__historically_underutilized",
                ),
                (
                    "sba_certified_8a_joint_venture",
                    "latest_transaction__contract_data__sba_certified_8_a_joint_ve",
                ),
                ("highly_compensated_officer_1_name", "officer_1_name"),
                ("highly_compensated_officer_1_amount", "officer_1_amount"),
                ("highly_compensated_officer_2_name", "officer_2_name"),
                ("highly_compensated_officer_2_amount", "officer_2_amount"),
                ("highly_compensated_officer_3_name", "officer_3_name"),
                ("highly_compensated_officer_3_amount", "officer_3_amount"),
                ("highly_compensated_officer_4_name", "officer_4_name"),
                ("highly_compensated_officer_4_amount", "officer_4_amount"),
                ("highly_compensated_officer_5_name", "officer_5_name"),
                ("highly_compensated_officer_5_amount", "officer_5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("last_modified_date", "latest_transaction__contract_data__last_modified"),
            ]
        ),
        "d2": OrderedDict(
            [
                ("assistance_award_unique_key", "generated_unique_award_id"),
                ("award_id_fain", "fain"),
                ("award_id_uri", "uri"),
                ("sai_number", "latest_transaction__assistance_data__sai_number"),
                (
                    "disaster_emergency_fund_codes" + NAMING_CONFLICT_DISCRIMINATOR,
                    None,
                ),  # Annotation is used to create this column
                ("outlayed_amount_funded_by_COVID-19_supplementals", None),  # Annotation is used to create this column
                ("obligated_amount_funded_by_COVID-19_supplementals", None),  # Annotation is used to create this column
                (
                    "award_latest_action_date",
                    "latest_transaction__action_date",
                ),  # Annotation is used to create this column
                ("award_latest_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("total_obligated_amount", "total_obligation"),
                ("indirect_cost_federal_share_amount", "total_indirect_federal_sharing"),
                ("total_non_federal_funding_amount", "non_federal_funding_amount"),
                ("total_funding_amount", "total_funding_amount"),
                ("total_face_value_of_loan", "total_loan_value"),
                ("total_loan_subsidy_cost", "total_subsidy_cost"),
                ("award_base_action_date", "date_signed"),
                ("award_base_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("period_of_performance_start_date", "period_of_performance_start_date"),
                ("period_of_performance_current_end_date", "period_of_performance_current_end_date"),
                ("awarding_agency_code", "latest_transaction__assistance_data__awarding_agency_code"),
                ("awarding_agency_name", "latest_transaction__assistance_data__awarding_agency_name"),
                ("awarding_sub_agency_code", "latest_transaction__assistance_data__awarding_sub_tier_agency_c"),
                ("awarding_sub_agency_name", "latest_transaction__assistance_data__awarding_sub_tier_agency_n"),
                ("awarding_office_code", "latest_transaction__assistance_data__awarding_office_code"),
                ("awarding_office_name", "latest_transaction__assistance_data__awarding_office_name"),
                ("funding_agency_code", "latest_transaction__assistance_data__funding_agency_code"),
                ("funding_agency_name", "latest_transaction__assistance_data__funding_agency_name"),
                ("funding_sub_agency_code", "latest_transaction__assistance_data__funding_sub_tier_agency_co"),
                ("funding_sub_agency_name", "latest_transaction__assistance_data__funding_sub_tier_agency_na"),
                ("funding_office_code", "latest_transaction__assistance_data__funding_office_code"),
                ("funding_office_name", "latest_transaction__assistance_data__funding_office_name"),
                ("treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("recipient_uei", "latest_transaction__assistance_data__uei"),
                ("recipient_duns", "latest_transaction__assistance_data__awardee_or_recipient_uniqu"),
                ("recipient_name", "latest_transaction__assistance_data__awardee_or_recipient_legal"),
                ("recipient_parent_uei", "latest_transaction__assistance_data__ultimate_parent_uei"),
                ("recipient_parent_duns", "latest_transaction__assistance_data__ultimate_parent_unique_ide"),
                ("recipient_parent_name", "latest_transaction__assistance_data__ultimate_parent_legal_enti"),
                ("recipient_country_code", "latest_transaction__assistance_data__legal_entity_country_code"),
                ("recipient_country_name", "latest_transaction__assistance_data__legal_entity_country_name"),
                ("recipient_address_line_1", "latest_transaction__assistance_data__legal_entity_address_line1"),
                ("recipient_address_line_2", "latest_transaction__assistance_data__legal_entity_address_line2"),
                ("recipient_city_code", "latest_transaction__assistance_data__legal_entity_city_code"),
                ("recipient_city_name", "latest_transaction__assistance_data__legal_entity_city_name"),
                ("recipient_county_code", "latest_transaction__assistance_data__legal_entity_county_code"),
                ("recipient_county_name", "latest_transaction__assistance_data__legal_entity_county_name"),
                ("recipient_state_code", "latest_transaction__assistance_data__legal_entity_state_code"),
                ("recipient_state_name", "latest_transaction__assistance_data__legal_entity_state_name"),
                ("recipient_zip_code", "latest_transaction__assistance_data__legal_entity_zip5"),
                ("recipient_zip_last_4_code", "latest_transaction__assistance_data__legal_entity_zip_last4"),
                (
                    "recipient_congressional_district",
                    "latest_transaction__assistance_data__legal_entity_congressional",
                ),
                (
                    "recipient_foreign_city_name",
                    "latest_transaction__assistance_data__legal_entity_foreign_city",
                ),
                (
                    "recipient_foreign_province_name",
                    "latest_transaction__assistance_data__legal_entity_foreign_provi",
                ),
                (
                    "recipient_foreign_postal_code",
                    "latest_transaction__assistance_data__legal_entity_foreign_posta",
                ),
                (
                    "primary_place_of_performance_scope",
                    "latest_transaction__assistance_data__place_of_performance_scope",
                ),
                (
                    "primary_place_of_performance_country_code",
                    "latest_transaction__assistance_data__place_of_perform_country_c",
                ),
                (
                    "primary_place_of_performance_country_name",
                    "latest_transaction__assistance_data__place_of_perform_country_n",
                ),
                (
                    "primary_place_of_performance_code",
                    "latest_transaction__assistance_data__place_of_performance_code",
                ),
                (
                    "primary_place_of_performance_city_name",
                    "latest_transaction__assistance_data__place_of_performance_city",
                ),
                (
                    "primary_place_of_performance_county_code",
                    "latest_transaction__assistance_data__place_of_perform_county_co",
                ),
                (
                    "primary_place_of_performance_county_name",
                    "latest_transaction__assistance_data__place_of_perform_county_na",
                ),
                (
                    "primary_place_of_performance_state_name",
                    "latest_transaction__assistance_data__place_of_perform_state_nam",
                ),
                (
                    "primary_place_of_performance_zip_4",
                    "latest_transaction__assistance_data__place_of_performance_zip4a",
                ),
                (
                    "primary_place_of_performance_congressional_district",
                    "latest_transaction__assistance_data__place_of_performance_congr",
                ),
                (
                    "primary_place_of_performance_foreign_location",
                    "latest_transaction__assistance_data__place_of_performance_forei",
                ),
                ("cfda_numbers_and_titles", None),  # Annotation is used to create this column
                ("funding_opportunity_number", "latest_transaction__assistance_data__funding_opportunity_number"),
                ("funding_opportunity_goals_text", "latest_transaction__assistance_data__funding_opportunity_goals"),
                ("assistance_type_code", "latest_transaction__assistance_data__assistance_type"),
                ("assistance_type_description", "latest_transaction__assistance_data__assistance_type_desc"),
                ("prime_award_base_transaction_description", "description"),
                (
                    "business_funds_indicator_code",
                    "latest_transaction__assistance_data__business_funds_indicator",
                ),
                (
                    "business_funds_indicator_description",
                    "latest_transaction__assistance_data__business_funds_ind_desc",
                ),
                ("business_types_code", "latest_transaction__assistance_data__business_types"),
                ("business_types_description", "latest_transaction__assistance_data__business_types_desc"),
                ("record_type_code", "latest_transaction__assistance_data__record_type"),
                ("record_type_description", "latest_transaction__assistance_data__record_type_description"),
                ("highly_compensated_officer_1_name", "officer_1_name"),
                ("highly_compensated_officer_1_amount", "officer_1_amount"),
                ("highly_compensated_officer_2_name", "officer_2_name"),
                ("highly_compensated_officer_2_amount", "officer_2_amount"),
                ("highly_compensated_officer_3_name", "officer_3_name"),
                ("highly_compensated_officer_3_amount", "officer_3_amount"),
                ("highly_compensated_officer_4_name", "officer_4_name"),
                ("highly_compensated_officer_4_amount", "officer_4_amount"),
                ("highly_compensated_officer_5_name", "officer_5_name"),
                ("highly_compensated_officer_5_amount", "officer_5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("last_modified_date", "latest_transaction__assistance_data__modified_at"),
            ]
        ),
    },
    "transaction_search": {
        "d1": OrderedDict(
            [
                ("contract_transaction_unique_key", "transaction__contract_data__detached_award_proc_unique"),
                ("contract_award_unique_key", "transaction__award__generated_unique_award_id"),
                ("award_id_piid", "transaction__contract_data__piid"),
                ("modification_number", "transaction__contract_data__award_modification_amendme"),
                ("transaction_number", "transaction__contract_data__transaction_number"),
                ("parent_award_agency_id", "transaction__contract_data__referenced_idv_agency_iden"),
                ("parent_award_agency_name", "transaction__contract_data__referenced_idv_agency_desc"),
                ("parent_award_id_piid", "transaction__contract_data__parent_award_id"),
                ("parent_award_modification_number", "transaction__contract_data__referenced_idv_modificatio"),
                ("federal_action_obligation", "transaction__federal_action_obligation"),
                ("total_dollars_obligated", "transaction__contract_data__total_obligated_amount"),
                ("base_and_exercised_options_value", "transaction__contract_data__base_exercised_options_val"),
                ("current_total_value_of_award", "transaction__contract_data__current_total_value_award"),
                ("base_and_all_options_value", "transaction__contract_data__base_and_all_options_value"),
                ("potential_total_value_of_award", "transaction__contract_data__potential_total_value_awar"),
                ("disaster_emergency_fund_codes_for_overall_award", None),  # Annotation is used to create this column
                (
                    "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award",
                    None,
                ),  # Annotation is used to create this column
                (
                    "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award",
                    None,
                ),  # Annotation is used to create this column
                ("action_date", "transaction__action_date"),
                ("action_date_fiscal_year", None),  # Annotation is used to create this column
                ("period_of_performance_start_date", "transaction__contract_data__period_of_performance_star"),
                ("period_of_performance_current_end_date", "transaction__contract_data__period_of_performance_curr"),
                ("period_of_performance_potential_end_date", "transaction__contract_data__period_of_perf_potential_e"),
                ("ordering_period_end_date", "transaction__contract_data__ordering_period_end_date"),
                ("solicitation_date", "transaction__contract_data__solicitation_date"),
                ("awarding_agency_code", "transaction__contract_data__awarding_agency_code"),
                ("awarding_agency_name", "transaction__contract_data__awarding_agency_name"),
                ("awarding_sub_agency_code", "transaction__contract_data__awarding_sub_tier_agency_c"),
                ("awarding_sub_agency_name", "transaction__contract_data__awarding_sub_tier_agency_n"),
                ("awarding_office_code", "transaction__contract_data__awarding_office_code"),
                ("awarding_office_name", "transaction__contract_data__awarding_office_name"),
                ("funding_agency_code", "transaction__contract_data__funding_agency_code"),
                ("funding_agency_name", "transaction__contract_data__funding_agency_name"),
                ("funding_sub_agency_code", "transaction__contract_data__funding_sub_tier_agency_co"),
                ("funding_sub_agency_name", "transaction__contract_data__funding_sub_tier_agency_na"),
                ("funding_office_code", "transaction__contract_data__funding_office_code"),
                ("funding_office_name", "transaction__contract_data__funding_office_name"),
                ("treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("foreign_funding", "transaction__contract_data__foreign_funding"),
                ("foreign_funding_description", "transaction__contract_data__foreign_funding_desc"),
                ("sam_exception", "transaction__contract_data__sam_exception"),
                ("sam_exception_description", "transaction__contract_data__sam_exception_description"),
                ("recipient_uei", "transaction__contract_data__awardee_or_recipient_uei"),
                ("recipient_duns", "transaction__contract_data__awardee_or_recipient_uniqu"),
                ("recipient_name", "transaction__contract_data__awardee_or_recipient_legal"),
                ("recipient_doing_business_as_name", "transaction__contract_data__vendor_doing_as_business_n"),
                ("cage_code", "transaction__contract_data__cage_code"),
                ("recipient_parent_uei", "transaction__contract_data__ultimate_parent_uei"),
                ("recipient_parent_duns", "transaction__contract_data__ultimate_parent_unique_ide"),
                ("recipient_parent_name", "transaction__contract_data__ultimate_parent_legal_enti"),
                ("recipient_country_code", "transaction__contract_data__legal_entity_country_code"),
                ("recipient_country_name", "transaction__contract_data__legal_entity_country_name"),
                ("recipient_address_line_1", "transaction__contract_data__legal_entity_address_line1"),
                ("recipient_address_line_2", "transaction__contract_data__legal_entity_address_line2"),
                ("recipient_city_name", "transaction__contract_data__legal_entity_city_name"),
                ("recipient_county_name", "transaction__contract_data__legal_entity_county_name"),
                ("recipient_state_code", "transaction__contract_data__legal_entity_state_code"),
                ("recipient_state_name", "transaction__contract_data__legal_entity_state_descrip"),
                ("recipient_zip_4_code", "transaction__contract_data__legal_entity_zip4"),
                ("recipient_congressional_district", "transaction__contract_data__legal_entity_congressional"),
                ("recipient_phone_number", "transaction__contract_data__vendor_phone_number"),
                ("recipient_fax_number", "transaction__contract_data__vendor_fax_number"),
                ("primary_place_of_performance_country_code", "transaction__contract_data__place_of_perform_country_c"),
                ("primary_place_of_performance_country_name", "transaction__contract_data__place_of_perf_country_desc"),
                ("primary_place_of_performance_city_name", "transaction__contract_data__place_of_perform_city_name"),
                ("primary_place_of_performance_county_name", "transaction__contract_data__place_of_perform_county_na"),
                ("primary_place_of_performance_state_code", "transaction__contract_data__place_of_performance_state"),
                ("primary_place_of_performance_state_name", "transaction__contract_data__place_of_perfor_state_desc"),
                ("primary_place_of_performance_zip_4", "transaction__contract_data__place_of_performance_zip4a"),
                (
                    "primary_place_of_performance_congressional_district",
                    "transaction__contract_data__place_of_performance_congr",
                ),
                ("award_or_idv_flag", "transaction__contract_data__pulled_from"),
                (
                    "award_type_code",
                    "transaction__contract_data__contract_award_type",
                ),  # Column is appended to in account_download.py
                (
                    "award_type",
                    "transaction__contract_data__contract_award_type_desc",
                ),  # Column is appended to in account_download.py
                ("idv_type_code", "transaction__contract_data__idv_type"),
                ("idv_type", "transaction__contract_data__idv_type_description"),
                ("multiple_or_single_award_idv_code", "transaction__contract_data__multiple_or_single_award_i"),
                ("multiple_or_single_award_idv", "transaction__contract_data__multiple_or_single_aw_desc"),
                ("type_of_idc_code", "transaction__contract_data__type_of_idc"),
                ("type_of_idc", "transaction__contract_data__type_of_idc_description"),
                ("type_of_contract_pricing_code", "transaction__contract_data__type_of_contract_pricing"),
                ("type_of_contract_pricing", "transaction__contract_data__type_of_contract_pric_desc"),
                ("transaction_description", "transaction__contract_data__award_description"),
                ("prime_award_base_transaction_description", "transaction__award__description"),
                ("action_type_code", "transaction__action_type"),
                ("action_type", "transaction__action_type_description"),
                ("solicitation_identifier", "transaction__contract_data__solicitation_identifier"),
                ("number_of_actions", "transaction__contract_data__number_of_actions"),
                ("inherently_governmental_functions", "transaction__contract_data__inherently_government_func"),
                (
                    "inherently_governmental_functions_description",
                    "transaction__contract_data__inherently_government_desc",
                ),
                ("product_or_service_code", "transaction__contract_data__product_or_service_code"),
                ("product_or_service_code_description", "transaction__contract_data__product_or_service_co_desc"),
                ("contract_bundling_code", "transaction__contract_data__contract_bundling"),
                ("contract_bundling", "transaction__contract_data__contract_bundling_descrip"),
                ("dod_claimant_program_code", "transaction__contract_data__dod_claimant_program_code"),
                ("dod_claimant_program_description", "transaction__contract_data__dod_claimant_prog_cod_desc"),
                ("naics_code", "transaction__contract_data__naics"),
                ("naics_description", "transaction__contract_data__naics_description"),
                ("recovered_materials_sustainability_code", "transaction__contract_data__recovered_materials_sustai"),
                ("recovered_materials_sustainability", "transaction__contract_data__recovered_materials_s_desc"),
                ("domestic_or_foreign_entity_code", "transaction__contract_data__domestic_or_foreign_entity"),
                ("domestic_or_foreign_entity", "transaction__contract_data__domestic_or_foreign_e_desc"),
                ("dod_acquisition_program_code", "transaction__contract_data__program_system_or_equipmen"),
                ("dod_acquisition_program_description", "transaction__contract_data__program_system_or_equ_desc"),
                (
                    "information_technology_commercial_item_category_code",
                    "transaction__contract_data__information_technology_com",
                ),
                (
                    "information_technology_commercial_item_category",
                    "transaction__contract_data__information_technolog_desc",
                ),
                ("epa_designated_product_code", "transaction__contract_data__epa_designated_product"),
                ("epa_designated_product", "transaction__contract_data__epa_designated_produc_desc"),
                ("country_of_product_or_service_origin_code", "transaction__contract_data__country_of_product_or_serv"),
                ("country_of_product_or_service_origin", "transaction__contract_data__country_of_product_or_desc"),
                ("place_of_manufacture_code", "transaction__contract_data__place_of_manufacture"),
                ("place_of_manufacture", "transaction__contract_data__place_of_manufacture_desc"),
                ("subcontracting_plan_code", "transaction__contract_data__subcontracting_plan"),
                ("subcontracting_plan", "transaction__contract_data__subcontracting_plan_desc"),
                ("extent_competed_code", "transaction__contract_data__extent_competed"),
                ("extent_competed", "transaction__contract_data__extent_compete_description"),
                ("solicitation_procedures_code", "transaction__contract_data__solicitation_procedures"),
                ("solicitation_procedures", "transaction__contract_data__solicitation_procedur_desc"),
                ("type_of_set_aside_code", "transaction__contract_data__type_set_aside"),
                ("type_of_set_aside", "transaction__contract_data__type_set_aside_description"),
                ("evaluated_preference_code", "transaction__contract_data__evaluated_preference"),
                ("evaluated_preference", "transaction__contract_data__evaluated_preference_desc"),
                ("research_code", "transaction__contract_data__research"),
                ("research", "transaction__contract_data__research_description"),
                ("fair_opportunity_limited_sources_code", "transaction__contract_data__fair_opportunity_limited_s"),
                ("fair_opportunity_limited_sources", "transaction__contract_data__fair_opportunity_limi_desc"),
                ("other_than_full_and_open_competition_code", "transaction__contract_data__other_than_full_and_open_c"),
                ("other_than_full_and_open_competition", "transaction__contract_data__other_than_full_and_o_desc"),
                ("number_of_offers_received", "transaction__contract_data__number_of_offers_received"),
                (
                    "commercial_item_acquisition_procedures_code",
                    "transaction__contract_data__commercial_item_acquisitio",
                ),
                ("commercial_item_acquisition_procedures", "transaction__contract_data__commercial_item_acqui_desc"),
                (
                    "small_business_competitiveness_demonstration_program",
                    "transaction__contract_data__small_business_competitive",
                ),
                (
                    "simplified_procedures_for_certain_commercial_items_code",
                    "transaction__contract_data__commercial_item_test_progr",
                ),
                (
                    "simplified_procedures_for_certain_commercial_items",
                    "transaction__contract_data__commercial_item_test_desc",
                ),
                ("a76_fair_act_action_code", "transaction__contract_data__a_76_fair_act_action"),
                ("a76_fair_act_action", "transaction__contract_data__a_76_fair_act_action_desc"),
                ("fed_biz_opps_code", "transaction__contract_data__fed_biz_opps"),
                ("fed_biz_opps", "transaction__contract_data__fed_biz_opps_description"),
                ("local_area_set_aside_code", "transaction__contract_data__local_area_set_aside"),
                ("local_area_set_aside", "transaction__contract_data__local_area_set_aside_desc"),
                (
                    "price_evaluation_adjustment_preference_percent_difference",
                    "transaction__contract_data__price_evaluation_adjustmen",
                ),
                ("clinger_cohen_act_planning_code", "transaction__contract_data__clinger_cohen_act_planning"),
                ("clinger_cohen_act_planning", "transaction__contract_data__clinger_cohen_act_pla_desc"),
                (
                    "materials_supplies_articles_equipment_code",
                    "transaction__contract_data__materials_supplies_article",
                ),
                ("materials_supplies_articles_equipment", "transaction__contract_data__materials_supplies_descrip"),
                ("labor_standards_code", "transaction__contract_data__labor_standards"),
                ("labor_standards", "transaction__contract_data__labor_standards_descrip"),
                ("construction_wage_rate_requirements_code", "transaction__contract_data__construction_wage_rate_req"),
                ("construction_wage_rate_requirements", "transaction__contract_data__construction_wage_rat_desc"),
                ("interagency_contracting_authority_code", "transaction__contract_data__interagency_contracting_au"),
                ("interagency_contracting_authority", "transaction__contract_data__interagency_contract_desc"),
                ("other_statutory_authority", "transaction__contract_data__other_statutory_authority"),
                ("program_acronym", "transaction__contract_data__program_acronym"),
                (
                    "parent_award_type_code",
                    "transaction__contract_data__referenced_idv_type",
                ),  # Column is appended to in account_download.py
                (
                    "parent_award_type",
                    "transaction__contract_data__referenced_idv_type_desc",
                ),  # Column is appended to in account_download.py
                ("parent_award_single_or_multiple_code", "transaction__contract_data__referenced_mult_or_single"),
                ("parent_award_single_or_multiple", "transaction__contract_data__referenced_mult_or_si_desc"),
                ("major_program", "transaction__contract_data__major_program"),
                ("national_interest_action_code", "transaction__contract_data__national_interest_action"),
                ("national_interest_action", "transaction__contract_data__national_interest_desc"),
                ("cost_or_pricing_data_code", "transaction__contract_data__cost_or_pricing_data"),
                ("cost_or_pricing_data", "transaction__contract_data__cost_or_pricing_data_desc"),
                ("cost_accounting_standards_clause_code", "transaction__contract_data__cost_accounting_standards"),
                ("cost_accounting_standards_clause", "transaction__contract_data__cost_accounting_stand_desc"),
                ("government_furnished_property_code", "transaction__contract_data__government_furnished_prope"),
                ("government_furnished_property", "transaction__contract_data__government_furnished_desc"),
                ("sea_transportation_code", "transaction__contract_data__sea_transportation"),
                ("sea_transportation", "transaction__contract_data__sea_transportation_desc"),
                ("undefinitized_action_code", "transaction__contract_data__undefinitized_action"),
                ("undefinitized_action", "transaction__contract_data__undefinitized_action_desc"),
                ("consolidated_contract_code", "transaction__contract_data__consolidated_contract"),
                ("consolidated_contract", "transaction__contract_data__consolidated_contract_desc"),
                ("performance_based_service_acquisition_code", "transaction__contract_data__performance_based_service"),
                ("performance_based_service_acquisition", "transaction__contract_data__performance_based_se_desc"),
                ("multi_year_contract_code", "transaction__contract_data__multi_year_contract"),
                ("multi_year_contract", "transaction__contract_data__multi_year_contract_desc"),
                ("contract_financing_code", "transaction__contract_data__contract_financing"),
                ("contract_financing", "transaction__contract_data__contract_financing_descrip"),
                ("purchase_card_as_payment_method_code", "transaction__contract_data__purchase_card_as_payment_m"),
                ("purchase_card_as_payment_method", "transaction__contract_data__purchase_card_as_paym_desc"),
                (
                    "contingency_humanitarian_or_peacekeeping_operation_code",
                    "transaction__contract_data__contingency_humanitarian_o",
                ),
                (
                    "contingency_humanitarian_or_peacekeeping_operation",
                    "transaction__contract_data__contingency_humanitar_desc",
                ),
                ("alaskan_native_corporation_owned_firm", "transaction__contract_data__alaskan_native_owned_corpo"),
                ("american_indian_owned_business", "transaction__contract_data__american_indian_owned_busi"),
                ("indian_tribe_federally_recognized", "transaction__contract_data__indian_tribe_federally_rec"),
                ("native_hawaiian_organization_owned_firm", "transaction__contract_data__native_hawaiian_owned_busi"),
                ("tribally_owned_firm", "transaction__contract_data__tribally_owned_business"),
                ("veteran_owned_business", "transaction__contract_data__veteran_owned_business"),
                ("service_disabled_veteran_owned_business", "transaction__contract_data__service_disabled_veteran_o"),
                ("woman_owned_business", "transaction__contract_data__woman_owned_business"),
                ("women_owned_small_business", "transaction__contract_data__women_owned_small_business"),
                (
                    "economically_disadvantaged_women_owned_small_business",
                    "transaction__contract_data__economically_disadvantaged",
                ),
                ("joint_venture_women_owned_small_business", "transaction__contract_data__joint_venture_women_owned"),
                (
                    "joint_venture_economic_disadvantaged_women_owned_small_bus",
                    "transaction__contract_data__joint_venture_economically",
                ),
                ("minority_owned_business", "transaction__contract_data__minority_owned_business"),
                (
                    "subcontinent_asian_asian_indian_american_owned_business",
                    "transaction__contract_data__subcontinent_asian_asian_i",
                ),
                ("asian_pacific_american_owned_business", "transaction__contract_data__asian_pacific_american_own"),
                ("black_american_owned_business", "transaction__contract_data__black_american_owned_busin"),
                ("hispanic_american_owned_business", "transaction__contract_data__hispanic_american_owned_bu"),
                ("native_american_owned_business", "transaction__contract_data__native_american_owned_busi"),
                ("other_minority_owned_business", "transaction__contract_data__other_minority_owned_busin"),
                (
                    "contracting_officers_determination_of_business_size",
                    "transaction__contract_data__contracting_officers_desc",
                ),
                (
                    "contracting_officers_determination_of_business_size_code",
                    "transaction__contract_data__contracting_officers_deter",
                ),
                ("emerging_small_business", "transaction__contract_data__emerging_small_business"),
                (
                    "community_developed_corporation_owned_firm",
                    "transaction__contract_data__community_developed_corpor",
                ),
                ("labor_surplus_area_firm", "transaction__contract_data__labor_surplus_area_firm"),
                ("us_federal_government", "transaction__contract_data__us_federal_government"),
                (
                    "federally_funded_research_and_development_corp",
                    "transaction__contract_data__federally_funded_research",
                ),
                ("federal_agency", "transaction__contract_data__federal_agency"),
                ("us_state_government", "transaction__contract_data__us_state_government"),
                ("us_local_government", "transaction__contract_data__us_local_government"),
                ("city_local_government", "transaction__contract_data__city_local_government"),
                ("county_local_government", "transaction__contract_data__county_local_government"),
                ("inter_municipal_local_government", "transaction__contract_data__inter_municipal_local_gove"),
                ("local_government_owned", "transaction__contract_data__local_government_owned"),
                ("municipality_local_government", "transaction__contract_data__municipality_local_governm"),
                ("school_district_local_government", "transaction__contract_data__school_district_local_gove"),
                ("township_local_government", "transaction__contract_data__township_local_government"),
                ("us_tribal_government", "transaction__contract_data__us_tribal_government"),
                ("foreign_government", "transaction__contract_data__foreign_government"),
                ("organizational_type", "transaction__contract_data__organizational_type"),
                ("corporate_entity_not_tax_exempt", "transaction__contract_data__corporate_entity_not_tax_e"),
                ("corporate_entity_tax_exempt", "transaction__contract_data__corporate_entity_tax_exemp"),
                (
                    "partnership_or_limited_liability_partnership",
                    "transaction__contract_data__partnership_or_limited_lia",
                ),
                ("sole_proprietorship", "transaction__contract_data__sole_proprietorship"),
                ("small_agricultural_cooperative", "transaction__contract_data__small_agricultural_coopera"),
                ("international_organization", "transaction__contract_data__international_organization"),
                ("us_government_entity", "transaction__contract_data__us_government_entity"),
                ("community_development_corporation", "transaction__contract_data__community_development_corp"),
                ("domestic_shelter", "transaction__contract_data__domestic_shelter"),
                ("educational_institution", "transaction__contract_data__educational_institution"),
                ("foundation", "transaction__contract_data__foundation"),
                ("hospital_flag", "transaction__contract_data__hospital_flag"),
                ("manufacturer_of_goods", "transaction__contract_data__manufacturer_of_goods"),
                ("veterinary_hospital", "transaction__contract_data__veterinary_hospital"),
                ("hispanic_servicing_institution", "transaction__contract_data__hispanic_servicing_institu"),
                ("receives_contracts", "transaction__contract_data__contracts"),
                ("receives_financial_assistance", "transaction__contract_data__grants"),
                (
                    "receives_contracts_and_financial_assistance",
                    "transaction__contract_data__receives_contracts_and_gra",
                ),
                ("airport_authority", "transaction__contract_data__airport_authority"),
                ("council_of_governments", "transaction__contract_data__council_of_governments"),
                ("housing_authorities_public_tribal", "transaction__contract_data__housing_authorities_public"),
                ("interstate_entity", "transaction__contract_data__interstate_entity"),
                ("planning_commission", "transaction__contract_data__planning_commission"),
                ("port_authority", "transaction__contract_data__port_authority"),
                ("transit_authority", "transaction__contract_data__transit_authority"),
                ("subchapter_scorporation", "transaction__contract_data__subchapter_s_corporation"),
                ("limited_liability_corporation", "transaction__contract_data__limited_liability_corporat"),
                ("foreign_owned", "transaction__contract_data__foreign_owned_and_located"),
                ("for_profit_organization", "transaction__contract_data__for_profit_organization"),
                ("nonprofit_organization", "transaction__contract_data__nonprofit_organization"),
                ("other_not_for_profit_organization", "transaction__contract_data__other_not_for_profit_organ"),
                ("the_ability_one_program", "transaction__contract_data__the_ability_one_program"),
                ("private_university_or_college", "transaction__contract_data__private_university_or_coll"),
                (
                    "state_controlled_institution_of_higher_learning",
                    "transaction__contract_data__state_controlled_instituti",
                ),
                ("1862_land_grant_college", "transaction__contract_data__c1862_land_grant_college"),
                ("1890_land_grant_college", "transaction__contract_data__c1890_land_grant_college"),
                ("1994_land_grant_college", "transaction__contract_data__c1994_land_grant_college"),
                ("minority_institution", "transaction__contract_data__minority_institution"),
                ("historically_black_college", "transaction__contract_data__historically_black_college"),
                ("tribal_college", "transaction__contract_data__tribal_college"),
                ("alaskan_native_servicing_institution", "transaction__contract_data__alaskan_native_servicing_i"),
                ("native_hawaiian_servicing_institution", "transaction__contract_data__native_hawaiian_servicing"),
                ("school_of_forestry", "transaction__contract_data__school_of_forestry"),
                ("veterinary_college", "transaction__contract_data__veterinary_college"),
                ("dot_certified_disadvantage", "transaction__contract_data__dot_certified_disadvantage"),
                (
                    "self_certified_small_disadvantaged_business",
                    "transaction__contract_data__self_certified_small_disad",
                ),
                ("small_disadvantaged_business", "transaction__contract_data__small_disadvantaged_busine"),
                ("c8a_program_participant", "transaction__contract_data__c8a_program_participant"),
                (
                    "historically_underutilized_business_zone_hubzone_firm",
                    "transaction__contract_data__historically_underutilized",
                ),
                ("sba_certified_8a_joint_venture", "transaction__contract_data__sba_certified_8_a_joint_ve"),
                ("highly_compensated_officer_1_name", "transaction__contract_data__officer_1_name"),
                ("highly_compensated_officer_1_amount", "transaction__contract_data__officer_1_amount"),
                ("highly_compensated_officer_2_name", "transaction__contract_data__officer_2_name"),
                ("highly_compensated_officer_2_amount", "transaction__contract_data__officer_2_amount"),
                ("highly_compensated_officer_3_name", "transaction__contract_data__officer_3_name"),
                ("highly_compensated_officer_3_amount", "transaction__contract_data__officer_3_amount"),
                ("highly_compensated_officer_4_name", "transaction__contract_data__officer_4_name"),
                ("highly_compensated_officer_4_amount", "transaction__contract_data__officer_4_amount"),
                ("highly_compensated_officer_5_name", "transaction__contract_data__officer_5_name"),
                ("highly_compensated_officer_5_amount", "transaction__contract_data__officer_5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("last_modified_date", "transaction__contract_data__last_modified"),
            ]
        ),
        "d2": OrderedDict(
            [
                ("assistance_transaction_unique_key", "transaction__assistance_data__afa_generated_unique"),
                ("assistance_award_unique_key", "transaction__award__generated_unique_award_id"),
                ("award_id_fain", "transaction__assistance_data__fain"),
                ("modification_number", "transaction__modification_number"),
                ("award_id_uri", "transaction__assistance_data__uri"),
                ("sai_number", "transaction__assistance_data__sai_number"),
                ("federal_action_obligation", "transaction__federal_action_obligation"),
                ("total_obligated_amount", "transaction__award__total_obligation"),
                ("indirect_cost_federal_share_amount", "transaction__assistance_data__indirect_federal_sharing"),
                ("non_federal_funding_amount", "transaction__assistance_data__non_federal_funding_amount"),
                ("total_non_federal_funding_amount", "transaction__award__non_federal_funding_amount"),
                ("face_value_of_loan", "transaction__assistance_data__face_value_loan_guarantee"),
                ("original_loan_subsidy_cost", "transaction__original_loan_subsidy_cost"),
                ("total_face_value_of_loan", "transaction__award__total_loan_value"),
                ("total_loan_subsidy_cost", "transaction__award__total_subsidy_cost"),
                ("disaster_emergency_fund_codes_for_overall_award", None),  # Annotation is used to create this column
                (
                    "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award",
                    None,
                ),  # Annotation is used to create this column
                (
                    "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award",
                    None,
                ),  # Annotation is used to create this column
                ("action_date", "transaction__action_date"),
                ("action_date_fiscal_year", None),  # Annotation is used to create this column
                ("period_of_performance_start_date", "transaction__period_of_performance_start_date"),
                ("period_of_performance_current_end_date", "transaction__period_of_performance_current_end_date"),
                ("awarding_agency_code", "transaction__assistance_data__awarding_agency_code"),
                ("awarding_agency_name", "transaction__assistance_data__awarding_agency_name"),
                ("awarding_sub_agency_code", "transaction__assistance_data__awarding_sub_tier_agency_c"),
                ("awarding_sub_agency_name", "transaction__assistance_data__awarding_sub_tier_agency_n"),
                ("awarding_office_code", "transaction__assistance_data__awarding_office_code"),
                ("awarding_office_name", "transaction__assistance_data__awarding_office_name"),
                ("funding_agency_code", "transaction__assistance_data__funding_agency_code"),
                ("funding_agency_name", "transaction__assistance_data__funding_agency_name"),
                ("funding_sub_agency_code", "transaction__assistance_data__funding_sub_tier_agency_co"),
                ("funding_sub_agency_name", "transaction__assistance_data__funding_sub_tier_agency_na"),
                ("funding_office_code", "transaction__assistance_data__funding_office_code"),
                ("funding_office_name", "transaction__assistance_data__funding_office_name"),
                ("treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("recipient_uei", "transaction__assistance_data__uei"),
                ("recipient_duns", "transaction__assistance_data__awardee_or_recipient_uniqu"),
                ("recipient_name", "transaction__assistance_data__awardee_or_recipient_legal"),
                ("recipient_parent_uei", "transaction__assistance_data__ultimate_parent_uei"),
                ("recipient_parent_duns", "transaction__assistance_data__ultimate_parent_unique_ide"),
                ("recipient_parent_name", "transaction__assistance_data__ultimate_parent_legal_enti"),
                ("recipient_country_code", "transaction__assistance_data__legal_entity_country_code"),
                ("recipient_country_name", "transaction__assistance_data__legal_entity_country_name"),
                ("recipient_address_line_1", "transaction__assistance_data__legal_entity_address_line1"),
                ("recipient_address_line_2", "transaction__assistance_data__legal_entity_address_line2"),
                ("recipient_city_code", "transaction__assistance_data__legal_entity_city_code"),
                ("recipient_city_name", "transaction__assistance_data__legal_entity_city_name"),
                ("recipient_county_code", "transaction__assistance_data__legal_entity_county_code"),
                ("recipient_county_name", "transaction__assistance_data__legal_entity_county_name"),
                ("recipient_state_code", "transaction__assistance_data__legal_entity_state_code"),
                ("recipient_state_name", "transaction__assistance_data__legal_entity_state_name"),
                ("recipient_zip_code", "transaction__assistance_data__legal_entity_zip5"),
                ("recipient_zip_last_4_code", "transaction__assistance_data__legal_entity_zip_last4"),
                ("recipient_congressional_district", "transaction__assistance_data__legal_entity_congressional"),
                ("recipient_foreign_city_name", "transaction__assistance_data__legal_entity_foreign_city"),
                ("recipient_foreign_province_name", "transaction__assistance_data__legal_entity_foreign_provi"),
                ("recipient_foreign_postal_code", "transaction__assistance_data__legal_entity_foreign_posta"),
                ("primary_place_of_performance_scope", "transaction__assistance_data__place_of_performance_scope"),
                (
                    "primary_place_of_performance_country_code",
                    "transaction__assistance_data__place_of_perform_country_c",
                ),
                (
                    "primary_place_of_performance_country_name",
                    "transaction__assistance_data__place_of_perform_country_n",
                ),
                ("primary_place_of_performance_code", "transaction__assistance_data__place_of_performance_code"),
                ("primary_place_of_performance_city_name", "transaction__assistance_data__place_of_performance_city"),
                (
                    "primary_place_of_performance_county_code",
                    "transaction__assistance_data__place_of_perform_county_co",
                ),
                (
                    "primary_place_of_performance_county_name",
                    "transaction__assistance_data__place_of_perform_county_na",
                ),
                ("primary_place_of_performance_state_name", "transaction__assistance_data__place_of_perform_state_nam"),
                ("primary_place_of_performance_zip_4", "transaction__assistance_data__place_of_performance_zip4a"),
                (
                    "primary_place_of_performance_congressional_district",
                    "transaction__assistance_data__place_of_performance_congr",
                ),
                (
                    "primary_place_of_performance_foreign_location",
                    "transaction__assistance_data__place_of_performance_forei",
                ),
                ("cfda_number", "transaction__assistance_data__cfda_number"),
                ("cfda_title", "transaction__assistance_data__cfda_title"),
                ("funding_opportunity_number", "transaction__assistance_data__funding_opportunity_number"),
                ("funding_opportunity_goals_text", "transaction__assistance_data__funding_opportunity_goals"),
                ("assistance_type_code", "transaction__assistance_data__assistance_type"),
                ("assistance_type_description", "transaction__assistance_data__assistance_type_desc"),
                ("transaction_description", "transaction__assistance_data__award_description"),
                ("prime_award_base_transaction_description", "transaction__award__description"),
                ("business_funds_indicator_code", "transaction__assistance_data__business_funds_indicator"),
                ("business_funds_indicator_description", "transaction__assistance_data__business_funds_ind_desc"),
                ("business_types_code", "transaction__assistance_data__business_types"),
                ("business_types_description", "transaction__assistance_data__business_types_desc"),
                ("correction_delete_indicator_code", "transaction__assistance_data__correction_delete_indicatr"),
                ("correction_delete_indicator_description", "transaction__assistance_data__correction_delete_ind_desc"),
                ("action_type_code", "transaction__action_type"),
                ("action_type_description", "transaction__assistance_data__action_type_description"),
                ("record_type_code", "transaction__assistance_data__record_type"),
                ("record_type_description", "transaction__assistance_data__record_type_description"),
                ("highly_compensated_officer_1_name", "transaction__assistance_data__officer_1_name"),
                ("highly_compensated_officer_1_amount", "transaction__assistance_data__officer_1_amount"),
                ("highly_compensated_officer_2_name", "transaction__assistance_data__officer_2_name"),
                ("highly_compensated_officer_2_amount", "transaction__assistance_data__officer_2_amount"),
                ("highly_compensated_officer_3_name", "transaction__assistance_data__officer_3_name"),
                ("highly_compensated_officer_3_amount", "transaction__assistance_data__officer_3_amount"),
                ("highly_compensated_officer_4_name", "transaction__assistance_data__officer_4_name"),
                ("highly_compensated_officer_4_amount", "transaction__assistance_data__officer_4_amount"),
                ("highly_compensated_officer_5_name", "transaction__assistance_data__officer_5_name"),
                ("highly_compensated_officer_5_amount", "transaction__assistance_data__officer_5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("last_modified_date", "transaction__assistance_data__modified_at"),
            ]
        ),
    },
    "subaward_search": {
        "d1": OrderedDict(
            [
                ("prime_award_unique_key", "unique_award_key"),
                ("prime_award_piid", "award_id"),
                ("prime_award_parent_piid", "parent_award_id"),
                ("prime_award_amount", "award_amount"),
                ("prime_award_disaster_emergency_fund_codes", None),  # Annotation is used to create this column
                (
                    "prime_award_outlayed_amount_funded_by_COVID-19_supplementals",
                    None,
                ),  # Annotation is used to create this column
                (
                    "prime_award_obligated_amount_funded_by_COVID-19_supplementals",
                    None,
                ),  # Annotation is used to create this column
                ("prime_award_base_action_date", "action_date"),
                ("prime_award_base_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("prime_award_latest_action_date", "award__latest_transaction__action_date"),
                ("prime_award_latest_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("prime_award_period_of_performance_start_date", "award__period_of_performance_start_date"),
                ("prime_award_period_of_performance_current_end_date", "award__period_of_performance_current_end_date"),
                (
                    "prime_award_period_of_performance_potential_end_date",
                    None,
                ),  # Annotation is used to create this column
                ("prime_award_awarding_agency_code", "awarding_agency_code"),
                ("prime_award_awarding_agency_name", "awarding_agency_name"),
                ("prime_award_awarding_sub_agency_code", "awarding_sub_tier_agency_c"),
                ("prime_award_awarding_sub_agency_name", "awarding_sub_tier_agency_n"),
                ("prime_award_awarding_office_code", "awarding_office_code"),
                ("prime_award_awarding_office_name", "awarding_office_name"),
                ("prime_award_funding_agency_code", "funding_agency_code"),
                ("prime_award_funding_agency_name", "funding_agency_name"),
                ("prime_award_funding_sub_agency_code", "funding_sub_tier_agency_co"),
                ("prime_award_funding_sub_agency_name", "funding_sub_tier_agency_na"),
                ("prime_award_funding_office_code", "funding_office_code"),
                ("prime_award_funding_office_name", "funding_office_name"),
                ("prime_award_treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("prime_awardee_uei", "awardee_or_recipient_uei"),
                ("prime_awardee_duns", "awardee_or_recipient_uniqu"),
                ("prime_awardee_name", "awardee_or_recipient_legal"),
                ("prime_awardee_dba_name", "dba_name"),
                ("prime_awardee_parent_uei", "ultimate_parent_uei"),
                ("prime_awardee_parent_duns", "ultimate_parent_unique_ide"),
                ("prime_awardee_parent_name", "ultimate_parent_legal_enti"),
                ("prime_awardee_country_code", "legal_entity_country_code"),
                ("prime_awardee_country_name", "legal_entity_country_name"),
                ("prime_awardee_address_line_1", "legal_entity_address_line1"),
                ("prime_awardee_city_name", "legal_entity_city_name"),
                ("prime_awardee_county_name", "award__latest_transaction__contract_data__legal_entity_county_name"),
                ("prime_awardee_state_code", "legal_entity_state_code"),
                ("prime_awardee_state_name", "legal_entity_state_name"),
                ("prime_awardee_zip_code", "legal_entity_zip"),
                ("prime_awardee_congressional_district", "legal_entity_congressional"),
                ("prime_awardee_foreign_postal_code", "legal_entity_foreign_posta"),
                ("prime_awardee_business_types", "business_types"),
                ("prime_award_primary_place_of_performance_city_name", "place_of_perform_city_name"),
                ("prime_award_primary_place_of_performance_state_code", "place_of_perform_state_code"),
                ("prime_award_primary_place_of_performance_state_name", "place_of_perform_state_name"),
                (
                    "prime_award_primary_place_of_performance_address_zip_code",
                    "place_of_performance_zip",
                ),
                (
                    "prime_award_primary_place_of_performance_congressional_district",
                    "place_of_perform_congressio",
                ),
                (
                    "prime_award_primary_place_of_performance_country_code",
                    "place_of_perform_country_co",
                ),
                (
                    "prime_award_primary_place_of_performance_country_name",
                    "place_of_perform_country_na",
                ),
                ("prime_award_base_transaction_description", "award_description"),
                ("prime_award_project_title", "program_title"),
                ("prime_award_naics_code", "naics"),
                ("prime_award_naics_description", "naics_description"),
                (
                    "prime_award_national_interest_action_code",
                    "award__latest_transaction__contract_data__national_interest_action",
                ),
                (
                    "prime_award_national_interest_action",
                    "award__latest_transaction__contract_data__national_interest_desc",
                ),
                ("subaward_type", "subaward_type"),
                ("subaward_fsrs_report_id", "internal_id"),
                ("subaward_fsrs_report_year", "subaward_report_year"),
                ("subaward_fsrs_report_month", "subaward_report_month"),
                ("subaward_number", "subaward_number"),
                ("subaward_amount", "subaward_amount"),
                ("subaward_action_date", "sub_action_date"),
                ("subaward_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("subawardee_uei", "sub_awardee_or_recipient_uei"),
                ("subawardee_duns", "sub_awardee_or_recipient_uniqu"),
                ("subawardee_name", "sub_awardee_or_recipient_legal"),
                ("subawardee_dba_name", "sub_dba_name"),
                ("subawardee_parent_uei", "sub_ultimate_parent_uei"),
                ("subawardee_parent_duns", "sub_ultimate_parent_unique_ide"),
                ("subawardee_parent_name", "sub_ultimate_parent_legal_enti"),
                ("subawardee_country_code", "sub_legal_entity_country_code"),
                ("subawardee_country_name", "sub_legal_entity_country_name"),
                ("subawardee_address_line_1", "sub_legal_entity_address_line1"),
                ("subawardee_city_name", "sub_legal_entity_city_name"),
                ("subawardee_state_code", "sub_legal_entity_state_code"),
                ("subawardee_state_name", "sub_legal_entity_state_name"),
                ("subawardee_zip_code", "sub_legal_entity_zip"),
                ("subawardee_congressional_district", "sub_legal_entity_congressional"),
                ("subawardee_foreign_postal_code", "sub_legal_entity_foreign_posta"),
                ("subawardee_business_types", "sub_business_types"),
                ("subaward_primary_place_of_performance_address_line_1", "place_of_perform_street"),
                ("subaward_primary_place_of_performance_city_name", "sub_place_of_perform_city_name"),
                (
                    "subaward_primary_place_of_performance_state_code",
                    "sub_place_of_perform_state_code",
                ),
                (
                    "subaward_primary_place_of_performance_state_name",
                    "sub_place_of_perform_state_name",
                ),
                (
                    "subaward_primary_place_of_performance_address_zip_code",
                    "sub_place_of_performance_zip",
                ),
                (
                    "subaward_primary_place_of_performance_congressional_district",
                    "sub_place_of_perform_congressio",
                ),
                (
                    "subaward_primary_place_of_performance_country_code",
                    "sub_place_of_perform_country_co",
                ),
                (
                    "subaward_primary_place_of_performance_country_name",
                    "sub_place_of_perform_country_na",
                ),
                ("subaward_description", "subaward_description"),
                ("subawardee_highly_compensated_officer_1_name", "sub_high_comp_officer1_full_na"),
                ("subawardee_highly_compensated_officer_1_amount", "sub_high_comp_officer1_amount"),
                ("subawardee_highly_compensated_officer_2_name", "sub_high_comp_officer2_full_na"),
                ("subawardee_highly_compensated_officer_2_amount", "sub_high_comp_officer2_amount"),
                ("subawardee_highly_compensated_officer_3_name", "sub_high_comp_officer3_full_na"),
                ("subawardee_highly_compensated_officer_3_amount", "sub_high_comp_officer3_amount"),
                ("subawardee_highly_compensated_officer_4_name", "sub_high_comp_officer4_full_na"),
                ("subawardee_highly_compensated_officer_4_amount", "sub_high_comp_officer4_amount"),
                ("subawardee_highly_compensated_officer_5_name", "sub_high_comp_officer5_full_na"),
                ("subawardee_highly_compensated_officer_5_amount", "sub_high_comp_officer5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("subaward_fsrs_report_last_modified_date", "date_submitted"),
            ]
        ),
        "d2": OrderedDict(
            [
                ("prime_award_unique_key", "unique_award_key"),
                ("prime_award_fain", "award_id"),
                ("prime_award_amount", "award_amount"),
                ("prime_award_disaster_emergency_fund_codes", None),  # Annotation is used to create this column
                (
                    "prime_award_outlayed_amount_funded_by_COVID-19_supplementals",
                    None,
                ),  # Annotation is used to create this column
                (
                    "prime_award_obligated_amount_funded_by_COVID-19_supplementals",
                    None,
                ),  # Annotation is used to create this column
                ("prime_award_base_action_date", "action_date"),
                ("prime_award_base_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("prime_award_latest_action_date", "award__latest_transaction__action_date"),
                ("prime_award_latest_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("prime_award_period_of_performance_start_date", "award__period_of_performance_start_date"),
                ("prime_award_period_of_performance_current_end_date", "award__period_of_performance_current_end_date"),
                ("prime_award_awarding_agency_code", "awarding_agency_code"),
                ("prime_award_awarding_agency_name", "awarding_agency_name"),
                ("prime_award_awarding_sub_agency_code", "awarding_sub_tier_agency_c"),
                ("prime_award_awarding_sub_agency_name", "awarding_sub_tier_agency_n"),
                ("prime_award_awarding_office_code", "awarding_office_code"),
                ("prime_award_awarding_office_name", "awarding_office_name"),
                ("prime_award_funding_agency_code", "funding_agency_code"),
                ("prime_award_funding_agency_name", "funding_agency_name"),
                ("prime_award_funding_sub_agency_code", "funding_sub_tier_agency_co"),
                ("prime_award_funding_sub_agency_name", "funding_sub_tier_agency_na"),
                ("prime_award_funding_office_code", "funding_office_code"),
                ("prime_award_funding_office_name", "funding_office_name"),
                ("prime_award_treasury_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_federal_accounts_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_object_classes_funding_this_award", None),  # Annotation is used to create this column
                ("prime_award_program_activities_funding_this_award", None),  # Annotation is used to create this column
                ("prime_awardee_uei", "awardee_or_recipient_uei"),
                ("prime_awardee_duns", "awardee_or_recipient_uniqu"),
                ("prime_awardee_name", "awardee_or_recipient_legal"),
                ("prime_awardee_dba_name", "dba_name"),
                ("prime_awardee_parent_uei", "ultimate_parent_uei"),
                ("prime_awardee_parent_duns", "ultimate_parent_unique_ide"),
                ("prime_awardee_parent_name", "ultimate_parent_legal_enti"),
                ("prime_awardee_country_code", "legal_entity_country_code"),
                ("prime_awardee_country_name", "legal_entity_country_name"),
                ("prime_awardee_address_line_1", "legal_entity_address_line1"),
                ("prime_awardee_city_name", "legal_entity_city_name"),
                ("prime_awardee_county_name", "award__latest_transaction__assistance_data__legal_entity_county_name"),
                ("prime_awardee_state_code", "legal_entity_state_code"),
                ("prime_awardee_state_name", "legal_entity_state_name"),
                ("prime_awardee_zip_code", "legal_entity_zip"),
                ("prime_awardee_congressional_district", "legal_entity_congressional"),
                ("prime_awardee_foreign_postal_code", "legal_entity_foreign_posta"),
                ("prime_awardee_business_types", "business_types"),
                ("prime_award_primary_place_of_performance_scope", "place_of_perform_scope"),
                ("prime_award_primary_place_of_performance_city_name", "place_of_perform_city_name"),
                ("prime_award_primary_place_of_performance_state_code", "place_of_perform_state_code"),
                ("prime_award_primary_place_of_performance_state_name", "place_of_perform_state_name"),
                (
                    "prime_award_primary_place_of_performance_address_zip_code",
                    "place_of_performance_zip",
                ),
                (
                    "prime_award_primary_place_of_performance_congressional_district",
                    "place_of_perform_congressio",
                ),
                (
                    "prime_award_primary_place_of_performance_country_code",
                    "place_of_perform_country_co",
                ),
                (
                    "prime_award_primary_place_of_performance_country_name",
                    "place_of_perform_country_na",
                ),
                ("prime_award_base_transaction_description", "award_description"),
                ("prime_award_cfda_numbers_and_titles", None),  # Annotation is used to create this column
                ("subaward_type", "subaward_type"),
                ("subaward_fsrs_report_id", "internal_id"),
                ("subaward_fsrs_report_year", "subaward_report_year"),
                ("subaward_fsrs_report_month", "subaward_report_month"),
                ("subaward_number", "subaward_number"),
                ("subaward_amount", "subaward_amount"),
                ("subaward_action_date", "sub_action_date"),
                ("subaward_action_date_fiscal_year", None),  # Annotation is used to create this column
                ("subawardee_uei", "sub_awardee_or_recipient_uei"),
                ("subawardee_duns", "sub_awardee_or_recipient_uniqu"),
                ("subawardee_name", "sub_awardee_or_recipient_legal_raw"),
                ("subawardee_dba_name", "sub_dba_name"),
                ("subawardee_parent_uei", "sub_ultimate_parent_uei"),
                ("subawardee_parent_duns", "sub_ultimate_parent_unique_ide"),
                ("subawardee_parent_name", "sub_ultimate_parent_legal_enti_raw"),
                ("subawardee_country_code", "sub_legal_entity_country_code_raw"),
                ("subawardee_country_name", "sub_legal_entity_country_name_raw"),
                ("subawardee_address_line_1", "sub_legal_entity_address_line1"),
                ("subawardee_city_name", "sub_legal_entity_city_name"),
                ("subawardee_state_code", "sub_legal_entity_state_code"),
                ("subawardee_state_name", "sub_legal_entity_state_name"),
                ("subawardee_zip_code", "sub_legal_entity_zip"),
                ("subawardee_congressional_district", "sub_legal_entity_congressional_raw"),
                ("subawardee_foreign_postal_code", "sub_legal_entity_foreign_posta"),
                ("subawardee_business_types", "sub_business_types"),
                ("subaward_primary_place_of_performance_address_line_1", "place_of_perform_street"),
                ("subaward_primary_place_of_performance_city_name", "sub_place_of_perform_city_name"),
                (
                    "subaward_primary_place_of_performance_state_code",
                    "sub_place_of_perform_state_code",
                ),
                (
                    "subaward_primary_place_of_performance_state_name",
                    "sub_place_of_perform_state_name",
                ),
                (
                    "subaward_primary_place_of_performance_address_zip_code",
                    "sub_place_of_performance_zip",
                ),
                (
                    "subaward_primary_place_of_performance_congressional_district",
                    "sub_place_of_perform_congressio_raw",
                ),
                (
                    "subaward_primary_place_of_performance_country_code",
                    "sub_place_of_perform_country_co_raw",
                ),
                (
                    "subaward_primary_place_of_performance_country_name",
                    "sub_place_of_perform_country_na",
                ),
                ("subaward_description", "subaward_description"),
                ("subawardee_highly_compensated_officer_1_name", "sub_high_comp_officer1_full_na"),
                ("subawardee_highly_compensated_officer_1_amount", "sub_high_comp_officer1_amount"),
                ("subawardee_highly_compensated_officer_2_name", "sub_high_comp_officer2_full_na"),
                ("subawardee_highly_compensated_officer_2_amount", "sub_high_comp_officer2_amount"),
                ("subawardee_highly_compensated_officer_3_name", "sub_high_comp_officer3_full_na"),
                ("subawardee_highly_compensated_officer_3_amount", "sub_high_comp_officer3_amount"),
                ("subawardee_highly_compensated_officer_4_name", "sub_high_comp_officer4_full_na"),
                ("subawardee_highly_compensated_officer_4_amount", "sub_high_comp_officer4_amount"),
                ("subawardee_highly_compensated_officer_5_name", "sub_high_comp_officer5_full_na"),
                ("subawardee_highly_compensated_officer_5_amount", "sub_high_comp_officer5_amount"),
                ("usaspending_permalink", None),  # to be filled in by annotation
                ("subaward_fsrs_report_last_modified_date", "date_submitted"),
            ]
        ),
    },
    "gtas_balances": {
        "treasury_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account_identifier__federal_account__parent_toptier_agency__name"),
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                (
                    "allocation_transfer_agency_identifier_code",
                    "allocation_transfer_agency_identifier_code",
                ),  # Column is annotated in account_download.py
                ("agency_identifier_code", "agency_identifier_code"),  # Column is annotated in account_download.py
                (
                    "beginning_period_of_availability",
                    "beginning_period_of_availability",
                ),  # Column is annotated in account_download.py
                (
                    "ending_period_of_availability",
                    "ending_period_of_availability",
                ),  # Column is annotated in account_download.py
                ("availability_type_code", "availability_type_code"),  # Column is annotated in account_download.py
                ("main_account_code", "main_account_code"),  # Column is annotated in account_download.py
                ("sub_account_code", "sub_account_code"),  # Column is annotated in account_download.py
                ("treasury_account_symbol", "tas_rendering_label"),
                ("treasury_account_name", "treasury_account_identifier__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),  # Column is annotated in account_download.py
                (
                    "allocation_transfer_agency_identifier_name",
                    "allocation_transfer_agency_identifier_name",
                ),  # Column is annotated in account_download.py
                ("budget_function", "treasury_account_identifier__budget_function_title"),
                ("budget_subfunction", "treasury_account_identifier__budget_subfunction_title"),
                ("federal_account_symbol", "treasury_account_identifier__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account_identifier__federal_account__account_title"),
                ("disaster_emergency_fund_code", "disaster_emergency_fund__code"),
                ("disaster_emergency_fund_name", "disaster_emergency_fund__public_law"),
                ("budget_authority_appropriated_amount", "budget_authority_appropriation_amount_cpe"),
                ("total_other_budgetary_resources_amount", "other_budgetary_resources_amount_cpe"),
                ("total_budgetary_resources", "total_budgetary_resources_cpe"),
                ("prior_year_paid_obligation_recoveries", "prior_year_paid_obligation_recoveries"),
                ("anticipated_prior_year_obligation_recoveries", "anticipated_prior_year_obligation_recoveries"),
                ("obligations_incurred", "obligations_incurred_total_cpe"),
                ("unobligated_balance", "unobligated_balance_cpe"),
                ("gross_outlay_amount", "gross_outlay_amount_by_tas_cpe"),
            ]
        )
    },
    "account_balances": {
        "treasury_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account_identifier__funding_toptier_agency__name"),
                ("reporting_agency_name", "submission__reporting_agency_name"),
                ("submission_period", "submission_period"),  # Column is appended to in account_download.py
                (
                    "allocation_transfer_agency_identifier_code",
                    "treasury_account_identifier__allocation_transfer_agency_id",
                ),
                ("agency_identifier_code", "treasury_account_identifier__agency_id"),
                ("beginning_period_of_availability", "treasury_account_identifier__beginning_period_of_availability"),
                ("ending_period_of_availability", "treasury_account_identifier__ending_period_of_availability"),
                ("availability_type_code", "treasury_account_identifier__availability_type_code"),
                ("main_account_code", "treasury_account_identifier__main_account_code"),
                ("sub_account_code", "treasury_account_identifier__sub_account_code"),
                ("treasury_account_symbol", "treasury_account_identifier__tas_rendering_label"),
                ("treasury_account_name", "treasury_account_identifier__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),
                ("allocation_transfer_agency_identifier_name", "allocation_transfer_agency_identifier_name"),
                ("budget_function", "treasury_account_identifier__budget_function_title"),
                ("budget_subfunction", "treasury_account_identifier__budget_subfunction_title"),
                ("federal_account_symbol", "treasury_account_identifier__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account_identifier__federal_account__account_title"),
                (
                    "budget_authority_unobligated_balance_brought_forward",
                    "budget_authority_unobligated_balance_brought_forward_fyb",
                ),
                (
                    "adjustments_to_unobligated_balance_brought_forward",
                    "adjustments_to_unobligated_balance_brought_forward_cpe",
                ),
                ("budget_authority_appropriated_amount", "budget_authority_appropriated_amount_cpe"),
                ("borrowing_authority_amount", "borrowing_authority_amount_total_cpe"),
                ("contract_authority_amount", "contract_authority_amount_total_cpe"),
                (
                    "spending_authority_from_offsetting_collections_amount",
                    "spending_authority_from_offsetting_collections_amount_cpe",
                ),
                ("total_other_budgetary_resources_amount", "other_budgetary_resources_amount_cpe"),
                ("total_budgetary_resources", "total_budgetary_resources_amount_cpe"),
                ("obligations_incurred", "obligations_incurred_total_by_tas_cpe"),
                (
                    "deobligations_or_recoveries_or_refunds_from_prior_year",
                    "deobligations_recoveries_refunds_by_tas_cpe",
                ),
                ("unobligated_balance", "unobligated_balance_cpe"),
                ("gross_outlay_amount", "gross_outlay_amount"),  # Column is annotated in account_download.py
                ("status_of_budgetary_resources_total", "status_of_budgetary_resources_total_cpe"),
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
        "federal_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account_identifier__federal_account__parent_toptier_agency__name"),
                ("reporting_agency_name", "reporting_agency_name"),  # Column is annotated in account_download.py
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                ("federal_account_symbol", "treasury_account_identifier__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account_identifier__federal_account__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),
                ("budget_function", "budget_function"),  # Column is annotated in account_download.py
                ("budget_subfunction", "budget_subfunction"),  # Column is annotated in account_download.py
                (
                    "budget_authority_unobligated_balance_brought_forward",
                    "budget_authority_unobligated_balance_brought_forward",
                ),
                (
                    "adjustments_to_unobligated_balance_brought_forward",
                    "adjustments_to_unobligated_balance_brought_forward",
                ),
                ("budget_authority_appropriated_amount", "budget_authority_appropriated_amount"),
                ("borrowing_authority_amount", "borrowing_authority_amount"),
                ("contract_authority_amount", "contract_authority_amount"),
                (
                    "spending_authority_from_offsetting_collections_amount",
                    "spending_authority_from_offsetting_collections_amount",
                ),
                ("total_other_budgetary_resources_amount", "total_other_budgetary_resources_amount"),
                ("total_budgetary_resources", "total_budgetary_resources"),
                ("obligations_incurred", "obligations_incurred"),
                (
                    "deobligations_or_recoveries_or_refunds_from_prior_year",
                    "deobligations_or_recoveries_or_refunds_from_prior_year",
                ),
                ("unobligated_balance", "unobligated_balance"),
                ("gross_outlay_amount", "gross_outlay_amount"),  # Column is annotated in account_download.py
                ("status_of_budgetary_resources_total", "status_of_budgetary_resources_total"),
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
    },
    "object_class_program_activity": {
        "treasury_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account__funding_toptier_agency__name"),
                ("reporting_agency_name", "submission__reporting_agency_name"),
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                ("allocation_transfer_agency_identifier_code", "treasury_account__allocation_transfer_agency_id"),
                ("agency_identifier_code", "treasury_account__agency_id"),
                ("beginning_period_of_availability", "treasury_account__beginning_period_of_availability"),
                ("ending_period_of_availability", "treasury_account__ending_period_of_availability"),
                ("availability_type_code", "treasury_account__availability_type_code"),
                ("main_account_code", "treasury_account__main_account_code"),
                ("sub_account_code", "treasury_account__sub_account_code"),
                ("treasury_account_symbol", "treasury_account__tas_rendering_label"),
                ("treasury_account_name", "treasury_account__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),
                ("allocation_transfer_agency_identifier_name", "allocation_transfer_agency_identifier_name"),
                ("budget_function", "treasury_account__budget_function_title"),
                ("budget_subfunction", "treasury_account__budget_subfunction_title"),
                ("federal_account_symbol", "treasury_account__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account__federal_account__account_title"),
                ("program_activity_code", "program_activity__program_activity_code"),
                ("program_activity_name", "program_activity__program_activity_name"),
                ("object_class_code", "object_class__object_class"),
                ("object_class_name", "object_class__object_class_name"),
                ("direct_or_reimbursable_funding_source", "object_class__direct_reimbursable"),
                ("disaster_emergency_fund_code", "disaster_emergency_fund__code"),
                ("disaster_emergency_fund_name", "disaster_emergency_fund__title"),
                ("obligations_incurred", "obligations_incurred_by_program_object_class_cpe"),
                ("obligations_undelivered_orders_unpaid_total", "obligations_undelivered_orders_unpaid_total_cpe"),
                ("obligations_undelivered_orders_unpaid_total_FYB", "obligations_undelivered_orders_unpaid_total_fyb"),
                (
                    "USSGL480100_undelivered_orders_obligations_unpaid",
                    "ussgl480100_undelivered_orders_obligations_unpaid_cpe",
                ),
                (
                    "USSGL480100_undelivered_orders_obligations_unpaid_FYB",
                    "ussgl480100_undelivered_orders_obligations_unpaid_fyb",
                ),
                (
                    "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid",
                    "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe",
                ),
                ("obligations_delivered_orders_unpaid_total", "obligations_delivered_orders_unpaid_total_cpe"),
                ("obligations_delivered_orders_unpaid_total_FYB", "obligations_delivered_orders_unpaid_total_cpe"),
                (
                    "USSGL490100_delivered_orders_obligations_unpaid",
                    "ussgl490100_delivered_orders_obligations_unpaid_cpe",
                ),
                (
                    "USSGL490100_delivered_orders_obligations_unpaid_FYB",
                    "ussgl490100_delivered_orders_obligations_unpaid_fyb",
                ),
                (
                    "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid",
                    "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe",
                ),
                (
                    "gross_outlay_amount_FYB_to_period_end",
                    "gross_outlay_amount_FYB_to_period_end",
                ),  # Column is annotated in account_download.py
                ("gross_outlay_amount_FYB", "gross_outlay_amount_by_program_object_class_fyb"),
                (
                    "gross_outlays_undelivered_orders_prepaid_total",
                    "gross_outlays_undelivered_orders_prepaid_total_cpe",
                ),
                (
                    "gross_outlays_undelivered_orders_prepaid_total_FYB",
                    "gross_outlays_undelivered_orders_prepaid_total_cpe",
                ),
                (
                    "USSGL480200_undelivered_orders_obligations_prepaid_advanced",
                    "gross_outlays_delivered_orders_paid_total_cpe",
                ),
                (
                    "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB",
                    "gross_outlays_delivered_orders_paid_total_fyb",
                ),
                (
                    "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid",
                    "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe",
                ),
                ("gross_outlays_delivered_orders_paid_total", "gross_outlays_delivered_orders_paid_total_cpe"),
                ("gross_outlays_delivered_orders_paid_total_FYB", "gross_outlays_delivered_orders_paid_total_fyb"),
                ("USSGL490200_delivered_orders_obligations_paid", "ussgl490200_delivered_orders_obligations_paid_cpe"),
                (
                    "USSGL490800_authority_outlayed_not_yet_disbursed",
                    "ussgl490800_authority_outlayed_not_yet_disbursed_cpe",
                ),
                (
                    "USSGL490800_authority_outlayed_not_yet_disbursed_FYB",
                    "ussgl490800_authority_outlayed_not_yet_disbursed_fyb",
                ),
                (
                    "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid",
                    "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe",
                ),
                (
                    "deobligations_or_recoveries_or_refunds_from_prior_year",
                    "deobligations_recoveries_refund_pri_program_object_class_cpe",
                ),
                (
                    "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig",
                    "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe",
                ),
                (
                    "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig",
                    "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe",
                ),
                (
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                ),
                (
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL483100_undelivered_orders_obligations_transferred_unpaid",
                    "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe",
                ),
                (
                    "USSGL493100_delivered_orders_obligations_transferred_unpaid",
                    "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe",
                ),
                (
                    "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced",
                    "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe",
                ),
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
        "federal_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account__federal_account__parent_toptier_agency__name"),
                ("reporting_agency_name", "reporting_agency_name"),  # Column is annotated in account_download.py
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                ("agency_identifier_name", "agency_identifier_name"),
                ("budget_function", "budget_function"),  # Column is annotated in account_download.py
                ("budget_subfunction", "budget_subfunction"),  # Column is annotated in account_download.py
                ("federal_account_symbol", "treasury_account__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account__federal_account__account_title"),
                ("program_activity_code", "program_activity__program_activity_code"),
                ("program_activity_name", "program_activity__program_activity_name"),
                ("object_class_code", "object_class__object_class"),
                ("object_class_name", "object_class__object_class_name"),
                ("direct_or_reimbursable_funding_source", "object_class__direct_reimbursable"),
                ("disaster_emergency_fund_code", "disaster_emergency_fund__code"),
                ("disaster_emergency_fund_name", "disaster_emergency_fund__title"),
                ("obligations_incurred", "obligations_incurred"),
                ("obligations_undelivered_orders_unpaid_total", "obligations_undelivered_orders_unpaid_total_cpe"),
                ("obligations_undelivered_orders_unpaid_total_FYB", "obligations_undelivered_orders_unpaid_total_fyb"),
                (
                    "USSGL480100_undelivered_orders_obligations_unpaid",
                    "ussgl480100_undelivered_orders_obligations_unpaid_cpe",
                ),
                (
                    "USSGL480100_undelivered_orders_obligations_unpaid_FYB",
                    "ussgl480100_undelivered_orders_obligations_unpaid_fyb",
                ),
                (
                    "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid",
                    "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe",
                ),
                ("obligations_delivered_orders_unpaid_total", "obligations_delivered_orders_unpaid_total_cpe"),
                ("obligations_delivered_orders_unpaid_total_FYB", "obligations_delivered_orders_unpaid_total_cpe"),
                (
                    "USSGL490100_delivered_orders_obligations_unpaid",
                    "ussgl490100_delivered_orders_obligations_unpaid_cpe",
                ),
                (
                    "USSGL490100_delivered_orders_obligations_unpaid_FYB",
                    "ussgl490100_delivered_orders_obligations_unpaid_fyb",
                ),
                (
                    "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid",
                    "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe",
                ),
                (
                    "gross_outlay_amount_FYB_to_period_end",
                    "gross_outlay_amount_FYB_to_period_end",
                ),  # Column is annotated in account_download.py
                ("gross_outlay_amount_FYB", "gross_outlay_amount_by_program_object_class_fyb"),
                (
                    "gross_outlays_undelivered_orders_prepaid_total",
                    "gross_outlays_undelivered_orders_prepaid_total_cpe",
                ),
                (
                    "gross_outlays_undelivered_orders_prepaid_total_FYB",
                    "gross_outlays_undelivered_orders_prepaid_total_cpe",
                ),
                (
                    "USSGL480200_undelivered_orders_obligations_prepaid_advanced",
                    "gross_outlays_delivered_orders_paid_total_cpe",
                ),
                (
                    "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB",
                    "gross_outlays_delivered_orders_paid_total_fyb",
                ),
                (
                    "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid",
                    "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe",
                ),
                ("gross_outlays_delivered_orders_paid_total", "gross_outlays_delivered_orders_paid_total_cpe"),
                ("gross_outlays_delivered_orders_paid_total_FYB", "gross_outlays_delivered_orders_paid_total_fyb"),
                ("USSGL490200_delivered_orders_obligations_paid", "ussgl490200_delivered_orders_obligations_paid_cpe"),
                (
                    "USSGL490800_authority_outlayed_not_yet_disbursed",
                    "ussgl490800_authority_outlayed_not_yet_disbursed_cpe",
                ),
                (
                    "USSGL490800_authority_outlayed_not_yet_disbursed_FYB",
                    "ussgl490800_authority_outlayed_not_yet_disbursed_fyb",
                ),
                (
                    "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid",
                    "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe",
                ),
                (
                    "deobligations_or_recoveries_or_refunds_from_prior_year",
                    "deobligations_recoveries_refund_pri_program_object_class_cpe",
                ),
                (
                    "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig",
                    "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe",
                ),
                (
                    "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig",
                    "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe",
                ),
                (
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                ),
                (
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL483100_undelivered_orders_obligations_transferred_unpaid",
                    "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe",
                ),
                (
                    "USSGL493100_delivered_orders_obligations_transferred_unpaid",
                    "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe",
                ),
                (
                    "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced",
                    "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe",
                ),
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
    },
    # Financial Accounts by Awards
    "award_financial": {
        "treasury_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account__funding_toptier_agency__name"),
                ("reporting_agency_name", "submission__reporting_agency_name"),
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                ("allocation_transfer_agency_identifier_code", "treasury_account__allocation_transfer_agency_id"),
                ("agency_identifier_code", "treasury_account__agency_id"),
                ("beginning_period_of_availability", "treasury_account__beginning_period_of_availability"),
                ("ending_period_of_availability", "treasury_account__ending_period_of_availability"),
                ("availability_type_code", "treasury_account__availability_type_code"),
                ("main_account_code", "treasury_account__main_account_code"),
                ("sub_account_code", "treasury_account__sub_account_code"),
                ("treasury_account_symbol", "treasury_account__tas_rendering_label"),
                ("treasury_account_name", "treasury_account__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),
                ("allocation_transfer_agency_identifier_name", "allocation_transfer_agency_identifier_name"),
                ("budget_function", "treasury_account__budget_function_title"),
                ("budget_subfunction", "treasury_account__budget_subfunction_title"),
                ("federal_account_symbol", "treasury_account__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account__federal_account__account_title"),
                ("program_activity_code", "program_activity__program_activity_code"),
                ("program_activity_name", "program_activity__program_activity_name"),
                ("object_class_code", "object_class__object_class"),
                ("object_class_name", "object_class__object_class_name"),
                ("direct_or_reimbursable_funding_source", "object_class__direct_reimbursable"),
                ("disaster_emergency_fund_code", "disaster_emergency_fund__code"),
                ("disaster_emergency_fund_name", "disaster_emergency_fund__title"),
                ("transaction_obligated_amount", "transaction_obligated_amount"),
                (
                    "gross_outlay_amount_FYB_to_period_end",
                    "gross_outlay_amount_FYB_to_period_end",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                ),  # Column is annotated in account_download.py
                ("award_unique_key", "award__generated_unique_award_id"),
                ("award_id_piid", "piid"),
                ("parent_award_id_piid", "parent_award_id"),
                ("award_id_fain", "fain"),
                ("award_id_uri", "uri"),
                ("award_base_action_date", "award__date_signed"),
                (
                    "award_base_action_date_fiscal_year",
                    "award_base_action_date_fiscal_year",
                ),  # Column is annotated in account_download.py
                ("award_latest_action_date", "award__certified_date"),
                (
                    "award_latest_action_date_fiscal_year",
                    "award_latest_action_date_fiscal_year",
                ),  # Column is annotated in account_download.py
                ("period_of_performance_start_date", "award__period_of_performance_start_date"),
                ("period_of_performance_current_end_date", "award__period_of_performance_current_end_date"),
                ("ordering_period_end_date", "award__latest_transaction__contract_data__ordering_period_end_date"),
                ("award_type_code", "award_type_code"),  # Column is annotated in account_download.py
                ("award_type", "award_type"),  # Column is annotated in account_download.py
                ("idv_type_code", "award__latest_transaction__contract_data__idv_type"),
                ("idv_type", "award__latest_transaction__contract_data__idv_type_description"),
                ("prime_award_base_transaction_description", "award__description"),
                ("awarding_agency_code", "awarding_agency_code"),  # Column is annotated in account_download.py
                ("awarding_agency_name", "awarding_agency_name"),  # Column is annotated in account_download.py
                ("awarding_subagency_code", "awarding_subagency_code"),  # Column is annotated in account_download.py
                ("awarding_subagency_name", "awarding_subagency_name"),  # Column is annotated in account_download.py
                ("awarding_office_code", "awarding_office_code"),  # Column is annotated in account_download.py
                ("awarding_office_name", "awarding_office_name"),  # Column is annotated in account_download.py
                ("funding_agency_code", "funding_agency_code"),  # Column is annotated in account_download.py
                ("funding_agency_name", "funding_agency_name"),  # Column is annotated in account_download.py
                ("funding_sub_agency_code", "funding_sub_agency_code"),  # Column is annotated in account_download.py
                ("funding_sub_agency_name", "funding_sub_agency_name"),  # Column is annotated in account_download.py
                ("funding_office_code", "funding_office_code"),  # Column is annotated in account_download.py
                ("funding_office_name", "funding_office_name"),  # Column is annotated in account_download.py
                ("recipient_uei", "recipient_uei"),  # Column is annotated in account_download.py
                ("recipient_duns", "recipient_duns"),  # Column is annotated in account_download.py
                ("recipient_name", "recipient_name"),  # Column is annotated in account_download.py
                ("recipient_parent_uei", "recipient_parent_uei"),  # Column is annotated in account_download.py
                ("recipient_parent_duns", "recipient_parent_duns"),  # Column is annotated in account_download.py
                ("recipient_parent_name", "recipient_parent_name"),  # Column is annotated in account_download.py
                ("recipient_country", "recipient_country"),  # Column is annotated in account_download.py
                ("recipient_state", "recipient_state"),  # Column is annotated in account_download.py
                ("recipient_county", "recipient_county"),  # Column is annotated in account_download.py
                ("recipient_city", "recipient_city"),  # Column is annotated in account_download.py
                ("recipient_congressional_district", "recipient_congressional_district"),
                # Column is annotated in account_download.py
                ("recipient_zip_code", "recipient_zip_code"),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_country",
                    "primary_place_of_performance_country",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_state",
                    "primary_place_of_performance_state",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_county",
                    "primary_place_of_performance_county",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_congressional_district",
                    "primary_place_of_performance_congressional_district",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_zip_code",
                    "primary_place_of_performance_zip_code",
                ),  # Column is annotated in account_download.py
                ("cfda_number", "award__latest_transaction__assistance_data__cfda_number"),
                ("cfda_title", "award__latest_transaction__assistance_data__cfda_title"),
                ("product_or_service_code", "award__latest_transaction__contract_data__product_or_service_code"),
                (
                    "product_or_service_code_description",
                    "award__latest_transaction__contract_data__product_or_service_co_desc",
                ),
                ("naics_code", "award__latest_transaction__contract_data__naics"),
                ("naics_description", "award__latest_transaction__contract_data__naics_description"),
                ("national_interest_action_code", "award__latest_transaction__contract_data__national_interest_action"),
                ("national_interest_action", "award__latest_transaction__contract_data__national_interest_desc"),
                ("usaspending_permalink", "usaspending_permalink"),  # to be filled in by annotation
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
        "federal_account": OrderedDict(
            [
                ("owning_agency_name", "treasury_account__federal_account__parent_toptier_agency__name"),
                ("reporting_agency_name", "reporting_agency_name"),  # Column is annotated in account_download.py
                ("submission_period", "submission_period"),  # Column is annotated in account_download.py
                ("federal_account_symbol", "treasury_account__federal_account__federal_account_code"),
                ("federal_account_name", "treasury_account__federal_account__account_title"),
                ("agency_identifier_name", "agency_identifier_name"),
                ("budget_function", "budget_function"),  # Column is annotated in account_download.py
                ("budget_subfunction", "budget_subfunction"),  # Column is annotated in account_download.py
                ("program_activity_code", "program_activity__program_activity_code"),
                ("program_activity_name", "program_activity__program_activity_name"),
                ("object_class_code", "object_class__object_class"),
                ("object_class_name", "object_class__object_class_name"),
                ("direct_or_reimbursable_funding_source", "object_class__direct_reimbursable"),
                ("disaster_emergency_fund_code", "disaster_emergency_fund__code"),
                ("disaster_emergency_fund_name", "disaster_emergency_fund__title"),
                ("transaction_obligated_amount", "transaction_obligated_amount"),
                (
                    "gross_outlay_amount_FYB_to_period_end",
                    "gross_outlay_amount_FYB_to_period_end",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                ),  # Column is annotated in account_download.py
                (
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                ),  # Column is annotated in account_download.py
                ("award_unique_key", "award__generated_unique_award_id"),
                ("award_id_piid", "piid"),
                ("parent_award_id_piid", "parent_award_id"),
                ("award_id_fain", "fain"),
                ("award_id_uri", "uri"),
                ("award_base_action_date", "award__date_signed"),
                (
                    "award_base_action_date_fiscal_year",
                    "award_base_action_date_fiscal_year",
                ),  # Column is annotated in account_download.py
                ("award_latest_action_date", "award__certified_date"),
                (
                    "award_latest_action_date_fiscal_year",
                    "award_latest_action_date_fiscal_year",
                ),  # Column is annotated in account_download.py
                ("period_of_performance_start_date", "award__period_of_performance_start_date"),
                ("period_of_performance_current_end_date", "award__period_of_performance_current_end_date"),
                ("ordering_period_end_date", "award__latest_transaction__contract_data__ordering_period_end_date"),
                ("award_type_code", "award_type_code"),  # Column is annotated in account_download.py
                ("award_type", "award_type"),  # Column is annotated in account_download.py
                ("idv_type_code", "award__latest_transaction__contract_data__idv_type"),
                ("idv_type", "award__latest_transaction__contract_data__idv_type_description"),
                ("prime_award_base_transaction_description", "award__description"),
                ("awarding_agency_code", "awarding_agency_code"),  # Column is annotated in account_download.py
                ("awarding_agency_name", "awarding_agency_name"),  # Column is annotated in account_download.py
                ("awarding_subagency_code", "awarding_subagency_code"),  # Column is annotated in account_download.py
                ("awarding_subagency_name", "awarding_subagency_name"),  # Column is annotated in account_download.py
                ("awarding_office_code", "awarding_office_code"),  # Column is annotated in account_download.py
                ("awarding_office_name", "awarding_office_name"),  # Column is annotated in account_download.py
                ("funding_agency_code", "funding_agency_code"),  # Column is annotated in account_download.py
                ("funding_agency_name", "funding_agency_name"),  # Column is annotated in account_download.py
                ("funding_sub_agency_code", "funding_sub_agency_code"),  # Column is annotated in account_download.py
                ("funding_sub_agency_name", "funding_sub_agency_name"),  # Column is annotated in account_download.py
                ("funding_office_code", "funding_office_code"),  # Column is annotated in account_download.py
                ("funding_office_name", "funding_office_name"),  # Column is annotated in account_download.py
                ("recipient_uei", "recipient_uei"),  # Column is annotated in account_download.py
                ("recipient_duns", "recipient_duns"),  # Column is annotated in account_download.py
                ("recipient_name", "recipient_name"),  # Column is annotated in account_download.py
                ("recipient_parent_uei", "recipient_parent_uei"),  # Column is annotated in account_download.py
                ("recipient_parent_duns", "recipient_parent_duns"),  # Column is annotated in account_download.py
                ("recipient_parent_name", "recipient_parent_name"),  # Column is annotated in account_download.py
                ("recipient_country", "recipient_country"),  # Column is annotated in account_download.py
                ("recipient_state", "recipient_state"),  # Column is annotated in account_download.py
                ("recipient_county", "recipient_county"),  # Column is annotated in account_download.py
                ("recipient_city", "recipient_city"),  # Column is annotated in account_download.py
                ("recipient_congressional_district", "recipient_congressional_district"),
                # Column is annotated in account_download.py
                ("recipient_zip_code", "recipient_zip_code"),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_country",
                    "primary_place_of_performance_country",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_state",
                    "primary_place_of_performance_state",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_county",
                    "primary_place_of_performance_county",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_congressional_district",
                    "primary_place_of_performance_congressional_district",
                ),  # Column is annotated in account_download.py
                (
                    "primary_place_of_performance_zip_code",
                    "primary_place_of_performance_zip_code",
                ),  # Column is annotated in account_download.py
                ("cfda_number", "award__latest_transaction__assistance_data__cfda_number"),
                ("cfda_title", "award__latest_transaction__assistance_data__cfda_title"),
                ("product_or_service_code", "award__latest_transaction__contract_data__product_or_service_code"),
                (
                    "product_or_service_code_description",
                    "award__latest_transaction__contract_data__product_or_service_co_desc",
                ),
                ("naics_code", "award__latest_transaction__contract_data__naics"),
                ("naics_description", "award__latest_transaction__contract_data__naics_description"),
                ("national_interest_action_code", "award__latest_transaction__contract_data__national_interest_action"),
                ("national_interest_action", "award__latest_transaction__contract_data__national_interest_desc"),
                ("usaspending_permalink", "usaspending_permalink"),  # to be filled in by annotation
                (
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                    "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR,
                ),  # Column is annotated in account_download.py
            ]
        ),
    },
    "disaster": {
        "recipient": OrderedDict(
            [
                ("recipient", "recipient_name"),
                ("award_obligations", "award_obligations"),
                ("award_outlays", "award_outlays"),
                ("face_value_of_loans", "face_value_of_loans"),
                ("number_of_awards", "number_of_awards"),
            ]
        )
    },
}

# Transactions are identical to TransactionSearch but originate from TransactionNormalized
query_paths["transaction"] = {
    "d1": OrderedDict(
        [
            (k, v[13:] if v is not None and v.startswith("transaction__") else v)
            for k, v in query_paths["transaction_search"]["d1"].items()
        ]
    ),
    "d2": OrderedDict(
        [
            (k, v[13:] if v is not None and v.startswith("transaction__") else v)
            for k, v in query_paths["transaction_search"]["d2"].items()
        ]
    ),
}


# IDV Orders are identical to Award but only contain "d1"
query_paths["idv_orders"] = {"d1": copy.deepcopy(query_paths["award"]["d1"])}

# IDV Transactions are identical to Transactions but only contain "d1"
query_paths["idv_transaction_history"] = {"d1": copy.deepcopy(query_paths["transaction"]["d1"])}

# Assistance Transactions are identical to Transactions but only contain "d2"
query_paths["assistance_transaction_history"] = {"d2": copy.deepcopy(query_paths["transaction"]["d2"])}
