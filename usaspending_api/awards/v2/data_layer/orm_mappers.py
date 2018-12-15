from collections import OrderedDict

# For all *_FIELDS ordered dictionaries:
#  Key:Value => (DB field, API response field)

FPDS_AWARD_FIELDS = OrderedDict(
    [
        ("id", "id"),
        ("generated_unique_award_id", "generated_unique_award_id"),
        ("piid", "piid"),
        ("parent_award_piid", "parent_award_piid"),
        ("type", "type"),
        ("category", "category"),
        ("type_description", "type_description"),
        ("description", "description"),
        ("total_obligation", "total_obligation"),
        ("base_exercised_options_val", "base_exercised_options_val"),
        ("base_and_all_options_value", "base_and_all_options_value"),
        ("subaward_count", "subaward_count"),
        ("total_subaward_amount", "total_subaward_amount"),
        # extra fields
        ("recipient_id", "_lei"),
        ("latest_transaction_id", "_trx"),
        ("awarding_agency_id", "_awarding_agency"),
        ("funding_agency_id", "_funding_agency"),
    ]
)


FPDS_CONTRACT_FIELDS = OrderedDict(
    [
        ("idv_type_description", "idv_type_description"),
        ("type_of_idc_description", "type_of_idc_description"),
        ("referenced_idv_agency_iden", "referenced_idv_agency_iden"),
        ("multiple_or_single_aw_desc", "multiple_or_single_aw_desc"),
        ("solicitation_identifier", "solicitation_identifier"),
        ("solicitation_procedures", "solicitation_procedures"),
        ("number_of_offers_received", "number_of_offers_received"),
        ("extent_competed", "extent_competed"),
        ("other_than_full_and_o_desc", "other_than_full_and_o_desc"),
        ("type_set_aside_description", "type_set_aside_description"),
        ("commercial_item_acquisitio", "commercial_item_acquisitio"),
        ("commercial_item_test_desc", "commercial_item_test_desc"),
        ("evaluated_preference_desc", "evaluated_preference_desc"),
        ("fed_biz_opps_description", "fed_biz_opps_description"),
        ("small_business_competitive", "small_business_competitive"),
        ("fair_opportunity_limi_desc", "fair_opportunity_limi_desc"),
        ("product_or_service_code", "product_or_service_code"),
        ("product_or_service_co_desc", "product_or_service_co_desc"),
        ("naics", "naics"),
        ("dod_claimant_program_code", "dod_claimant_program_code"),
        ("program_system_or_equipmen", "program_system_or_equipmen"),
        ("information_technolog_desc", "information_technolog_desc"),
        ("sea_transportation_desc", "sea_transportation_desc"),
        ("clinger_cohen_act_pla_desc", "clinger_cohen_act_pla_desc"),
        ("construction_wage_rat_desc", "construction_wage_rat_desc"),
        ("labor_standards_descrip", "labor_standards_descrip"),
        ("materials_supplies_descrip", "materials_supplies_descrip"),
        ("cost_or_pricing_data_desc", "cost_or_pricing_data_desc"),
        ("domestic_or_foreign_e_desc", "domestic_or_foreign_e_desc"),
        ("foreign_funding_desc", "foreign_funding_desc"),
        ("interagency_contract_desc", "interagency_contract_desc"),
        ("major_program", "major_program"),
        ("price_evaluation_adjustmen", "price_evaluation_adjustmen"),
        ("program_acronym", "program_acronym"),
        ("subcontracting_plan", "subcontracting_plan"),
        ("multi_year_contract_desc", "multi_year_contract_desc"),
        ("purchase_card_as_paym_desc", "purchase_card_as_paym_desc"),
        ("consolidated_contract_desc", "consolidated_contract_desc"),
        ("type_of_contract_pric_desc", "type_of_contract_pric_desc"),

        # "Legal Entity" fields below
        ("awardee_or_recipient_legal", "_recipient_name"),
        ("awardee_or_recipient_uniqu", "_recipient_unique_id"),
        ("ultimate_parent_unique_ide", "_parent_recipient_unique_id"),

        # "Place of Performance Location"
        ("place_of_perform_country_c", "_country_code"),
        ("place_of_perf_country_desc", "_country_name"),
        ("place_of_performance_state", "_state_code"),
        ("place_of_perform_city_name", "_city_name"),
        ("place_of_perform_county_na", "_county_name"),
        ("place_of_performance_zip4a", "_zip4"),
        ("place_of_performance_congr", "_congressional_code"),
        ("place_of_performance_zip5", "_zip5"),
    ]
)


OFFICER_FIELDS = OrderedDict([
    ("officer_1_name", "officer_1_name"),
    ("officer_1_amount", "officer_1_amount"),
    ("officer_2_name", "officer_2_name"),
    ("officer_2_amount", "officer_2_amount"),
    ("officer_3_name", "officer_3_name"),
    ("officer_3_amount", "officer_3_amount"),
    ("officer_4_name", "officer_4_name"),
    ("officer_4_amount", "officer_4_amount"),
    ("officer_5_name", "officer_5_name"),
    ("officer_5_amount", "officer_5_amount"),
])
