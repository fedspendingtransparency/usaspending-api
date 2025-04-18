"""
 Mapping dictionaries
     - used for converting terse_labels from broker to semi-terse labels used in the datastore)
TERSE_TO_LONG was removed.  IF NEEDED, change LONG_TO_TERSE to a bidict or invert the dict."""

LONG_TO_TERSE_LABELS = {
    "allocation_transfer_agency_id": "allocation_transfer_agency_id",
    "agency_id": "responsible_agency_id",
    "beginning_period_of_availability": "beginning_period_of_availa",
    "ending_period_of_availability": "ending_period_of_availabil",
    "availability_type_code": "availability_type_code",
    "main_account_code": "main_account_code",
    "sub_account_code": "sub_account_code",
    "budget_authority_unobligated_balance_brought_forward_fyb": "budget_authority_unobligat_fyb",
    "adjustments_to_unobligated_balance_brought_forward_cpe": "adjustments_to_unobligated_cpe",
    "budget_authority_appropriated_amount_cpe": "budget_authority_appropria_cpe",
    "borrowing_authority_amount_total_cpe": "borrowing_authority_amount_cpe",
    "contract_authority_amount_total_cpe": "contract_authority_amount_cpe",
    "spending_authority_from_offsetting_collections_amount_cpe": "spending_authority_from_of_cpe",
    "other_budgetary_resources_amount_cpe": "other_budgetary_resources_cpe",
    "total_budgetary_resources_amount_cpe": "total_budgetary_resources_cpe",
    "gross_outlay_amount_by_tas_cpe": "gross_outlay_amount_by_tas_cpe",
    "obligations_incurred_total_by_tas_cpe": "obligations_incurred_total_cpe",
    "deobligations_recoveries_refunds_by_tas_cpe": "deobligations_recoveries_r_cpe",
    "unobligated_balance_cpe": "unobligated_balance_cpe",
    "status_of_budgetary_resources_total_cpe": "status_of_budgetary_resour_cpe",
    "piid": "piid",
    "awarding_sub_tier_agency_code": "awarding_sub_tier_agency_c",
    "awarding_sub_tier_agency_name": "awarding_sub_tier_agency_n",
    "awarding_agency_code": "awarding_agency_code",
    "awarding_agency_name": "awarding_agency_name",
    "parent_award_id": "parent_award_id",
    "modification_number": "award_modification_amendme",
    "type_of_contract_pricing": "type_of_contract_pricing",
    "contract_award_type": "contract_award_type",
    "naics": "naics",
    "naics_description": "naics_description",
    "recipient_unique_id": "awardee_or_recipient_uniqu",
    "ultimate_parent_name": "ultimate_parent_legal_enti",
    "parent_recipient_unique_id": "ultimate_parent_unique_ide",
    "award_description": "award_description",
    "primary_place_of_performance_zip4a": "place_of_performance_zip4a",
    "primary_place_of_performance_congressional_district": "place_of_performance_congr",
    "recipient_name": "awardee_or_recipient_legal",
    "recipient_city_name": "legal_entity_city_name",
    "recipient_state_description": "legal_entity_state_description",
    "recipient_zip4": "legal_entity_zip4",
    "recipient_congressional_district": "legal_entity_congressional",
    "recipient_address_line1": "legal_entity_address_line1",
    "recipient_address_line2": "legal_entity_address_line2",
    "recipient_address_line3": "legal_entity_address_line3",
    "recipient_country_code": "legal_entity_country_code",
    "recipient_country_name": "legal_entity_country_name",
    "period_of_performance_start_date": "period_of_performance_star",
    "period_of_performance_current_end_date": "period_of_performance_curr",
    "period_of_performance_potential_end_date": "period_of_perf_potential_e",
    "ordering_period_end_date": "ordering_period_end_date",
    "action_date": "action_date",
    "action_type": "action_type",
    "action_type_description": "action_type_description",
    "federal_action_obligation": "federal_action_obligation",
    "current_total_value_of_award": "current_total_value_award",
    "potential_total_value_of_award": "potential_total_value_awar",
    "funding_sub_tier_agency_code": "funding_sub_tier_agency_co",
    "funding_sub_tier_agency_name": "funding_sub_tier_agency_na",
    "funding_office_code": "funding_office_code",
    "funding_agency_office_name": "funding_office_name",
    "awarding_office_code": "awarding_office_code",
    "awarding_office_name": "awarding_office_name",
    "referenced_idv_agency_identifier": "referenced_idv_agency_iden",
    "funding_agency_code": "funding_agency_code",
    "funding_agency_name": "funding_agency_name",
    "primary_place_of_performance_location_code": "place_of_performance_locat",
    "primary_place_of_performance_state_code": "place_of_performance_state",
    "primary_place_of_performance_country_code": "place_of_perform_country_c",
    "idv_type": "idv_type",
    "vendor_doing_as_business_name": "vendor_doing_as_business_n",
    "vendor_phone_number": "vendor_phone_number",
    "vendor_fax_number": "vendor_fax_number",
    "multiple_or_single_award_idv": "multiple_or_single_award_i",
    "type_of_idc": "type_of_idc",
    "a76_fair_act_action": "a76_fair_act_action",
    "dod_claimant_program_code": "dod_claimant_program_code",
    "clinger_cohen_act_planning": "clinger_cohen_act_planning",
    "commercial_item_acquisition_procedures": "commercial_item_acquisitio",
    "commercial_item_test_program": "commercial_item_test_progr",
    "consolidated_contract": "consolidated_contract",
    "contingency_humanitarian_or_peacekeeping_operation": "contingency_humanitarian_o",
    "contract_bundling": "contract_bundling",
    "contract_financing": "contract_financing",
    "contracting_officers_determination_of_business_size": "contracting_officers_deter",
    "cost_accounting_standards": "cost_accounting_standards",
    "cost_or_pricing_data": "cost_or_pricing_data",
    "country_of_product_or_service_origin": "country_of_product_or_serv",
    "construction_wage_rate_requirements": "construction_wage_rate_req",
    "construction_wage_rate_requirements_description": "construction_wage_rat_desc",
    "evaluated_preference": "evaluated_preference",
    "extent_competed": "extent_competed",
    "fed_biz_opps": "fed_biz_opps",
    "foreign_funding": "foreign_funding",
    "gfe_gfp": "gfe_gfp",
    "information_technology_commercial_item_category": "information_technology_com",
    "interagency_contracting_authority": "interagency_contracting_au",
    "local_area_set_aside": "local_area_set_aside",
    "major_program": "major_program",
    "purchase_card_as_payment_method": "purchase_card_as_payment_m",
    "multi_year_contract": "multi_year_contract",
    "national_interest_action": "national_interest_action",
    "number_of_actions": "number_of_actions",
    "number_of_offers_received": "number_of_offers_received",
    "other_statutory_authority": "other_statutory_authority",
    "performance_based_service_acquisition": "performance_based_service",
    "place_of_manufacture": "place_of_manufacture",
    "price_evaluation_adjustment_preference_percent_difference": "price_evaluation_adjustmen",
    "product_or_service_code": "product_or_service_code",
    "program_acronym": "program_acronym",
    "other_than_full_and_open_competition": "other_than_full_and_open_c",
    "recovered_materials_sustainability": "recovered_materials_sustai",
    "research": "research",
    "sea_transportation": "sea_transportation",
    "labor_standards": "labor_standards",
    "labor_standards_description": "labor_standards_descrip",
    "small_business_competitiveness_demonstration _program": "small_business_competitive",
    "solicitation_identifier": "solicitation_identifier",
    "solicitation_procedures": "solicitation_procedures",
    "fair_opportunity_limited_sources": "fair_opportunity_limited_s",
    "subcontracting_plan": "subcontracting_plan",
    "program_system_or_equipment_code": "program_system_or_equipmen",
    "type_set_aside": "type_set_aside",
    "epa_designated_product": "epa_designated_product",
    "materials_supplies_article": "materials_supplies_article",
    "materials_supplies_description": "materials_supplies_descrip",
    "transaction_number": "transaction_number",
    "sam_exception": "sam_exception",
    "city_local_government": "city_local_government",
    "county_local_government": "county_local_government",
    "inter_municipal_local_government": "inter_municipal_local_gove",
    "local_government_owned": "local_government_owned",
    "municipality_local_government": "municipality_local_governm",
    "school_district_local_government": "school_district_local_gove",
    "township_local_government": "township_local_government",
    "us_state_government": "us_state_government",
    "us_federal_government": "us_federal_government",
    "federal_agency": "federal_agency",
    "federally_funded_research_and_development_corp": "federally_funded_research",
    "us_tribal_government": "us_tribal_government",
    "foreign_government": "foreign_government",
    "community_developed_corporation_owned_firm": "community_developed_corpor",
    "labor_surplus_area_firm": "labor_surplus_area_firm",
    "corporate_entity_not_tax_exempt": "corporate_entity_not_tax_e",
    "corporate_entity_tax_exempt": "corporate_entity_tax_exemp",
    "partnership_or_limited_liability_partnership": "partnership_or_limited_lia",
    "sole_proprietorship": "sole_proprietorship",
    "small_agricultural_cooperative": "small_agricultural_coopera",
    "international_organization": "international_organization",
    "us_government_entity": "us_government_entity",
    "emerging_small_business": "emerging_small_business",
    "c8a_program_participant": "c8a_program_participant",
    "sba_certified_8a_joint_venture": "sba_certified_8a_joint_venture",
    "dot_certified_disadvantage": "dot_certified_disadvantage",
    "self_certified_small_disadvantaged_business": "self_certified_small_disad",
    "historically_underutilized_business_zone _hubzone_firm": "historically_underutilized",
    "small_disadvantaged_business": "small_disadvantaged_busine",
    "the_ability_one_program": "the_ability_one_program",
    "historically_black_college": "historically_black_college",
    "c1862_land_grant_college": "c1862_land_grant_college",
    "c1890_land_grant_college": "c1890_land_grant_college",
    "c1994_land_grant_college": "c1994_land_grant_college",
    "minority_institution": "minority_institution",
    "private_university_or_college": "private_university_or_coll",
    "school_of_forestry": "school_of_forestry",
    "state_controlled_institution_of_higher_learning": "state_controlled_instituti",
    "tribal_college": "tribal_college",
    "veterinary_college": "veterinary_college",
    "educational_institution": "educational_institution",
    "alaskan_native_servicing_institution": "alaskan_native_servicing_i",
    "community_development_corporation": "community_development_corp",
    "native_hawaiian_servicing_institution": "native_hawaiian_servicing",
    "domestic_shelter": "domestic_shelter",
    "manufacturer_of_goods": "manufacturer_of_goods",
    "hospital_flag": "hospital_flag",
    "veterinary_hospital": "veterinary_hospital",
    "hispanic_servicing_institution": "hispanic_servicing_institu",
    "foundation": "foundation",
    "woman_owned_business": "woman_owned_business",
    "minority_owned_business": "minority_owned_business",
    "women_owned_small_business": "women_owned_small_business",
    "economically_disadvantaged_women_owned_small_business": "economically_disadvantaged",
    "joint_venture_women_owned_small_business": "joint_venture_women_owned",
    "joint_venture_economic_disadvantaged_women_owned_small_bus": "joint_venture_economically",
    "veteran_owned_business": "veteran_owned_business",
    "service_disabled_veteran_owned_business": "service_disabled_veteran_o",
    "contracts": "contracts",
    "grants": "grants",
    "receives_contracts_and_grants": "receives_contracts_and_gra",
    "airport_authority": "airport_authority",
    "council_of_governments": "council_of_governments",
    "housing_authorities_public_tribal": "housing_authorities_public",
    "interstate_entity": "interstate_entity",
    "planning_commission": "planning_commission",
    "port_authority": "port_authority",
    "transit_authority": "transit_authority",
    "subchapter_scorporation": "subchapter_scorporation",
    "limited_liability_corporation": "limited_liability_corporat",
    "foreign_owned_and_located": "foreign_owned_and_located",
    "american_indian_owned_business": "american_indian_owned_busi",
    "alaskan_native_owned_corporation_or_firm": "alaskan_native_owned_corpo",
    "indian_tribe_federally_recognized": "indian_tribe_federally_rec",
    "native_hawaiian_owned_business": "native_hawaiian_owned_busi",
    "tribally_owned_business": "tribally_owned_business",
    "asian_pacific_american_owned_business": "asian_pacific_american_own",
    "black_american_owned_business": "black_american_owned_busin",
    "hispanic_american_owned_business": "hispanic_american_owned_bu",
    "native_american_owned_business": "native_american_owned_busi",
    "subcontinent_asian_asian_indian_american_owned_business": "subcontinent_asian_asian_i",
    "other_minority_owned_business": "other_minority_owned_busin",
    "for_profit_organization": "for_profit_organization",
    "nonprofit_organization": "nonprofit_organization",
    "other_not_for_profit_organization": "other_not_for_profit_organ",
    "us_local_government": "us_local_government",
    "referenced_idv_modification_number": "referenced_idv_modificatio",
    "undefinitized_action": "undefinitized_action",
    "domestic_or_foreign_entity": "domestic_or_foreign_entity",
    "fain": "fain",
    "uri": "uri",
    "transaction_obligated_amount": "transaction_obligated_amou",
    "ussgl480100_undelivered_orders_obligations_unpaid_fyb": "ussgl480100_undelivered_or_fyb",
    "ussgl480100_undelivered_orders_obligations_unpaid_cpe": "ussgl480100_undelivered_or_cpe",
    "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe": "ussgl483100_undelivered_or_cpe",
    "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": "ussgl488100_upward_adjustm_cpe",
    "obligations_undelivered_orders_unpaid_total_fyb": "obligations_undelivered_or_fyb",
    "obligations_undelivered_orders_unpaid_total_cpe": "obligations_undelivered_or_cpe",
    "ussgl490100_delivered_orders_obligations_unpaid_fyb": "ussgl490100_delivered_orde_fyb",
    "ussgl490100_delivered_orders_obligations_unpaid_cpe": "ussgl490100_delivered_orde_cpe",
    "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe": "ussgl493100_delivered_orde_cpe",
    "obligations_delivered_orders_unpaid_total_fyb": "obligations_delivered_orde_fyb",
    "obligations_delivered_orders_unpaid_total_cpe": "obligations_delivered_orde_cpe",
    "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb": "ussgl480200_undelivered_or_fyb",
    "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": "ussgl480200_undelivered_or_cpe",
    "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe": "ussgl483200_undelivered_or_cpe",
    "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": "ussgl488200_upward_adjustm_cpe",
    "gross_outlays_undelivered_orders_prepaid_total_fyb": "gross_outlays_undelivered_fyb",
    "gross_outlays_undelivered_orders_prepaid_total_cpe": "gross_outlays_undelivered_cpe",
    "ussgl490200_delivered_orders_obligations_paid_cpe": "ussgl490200_delivered_orde_cpe",
    "ussgl490800_authority_outlayed_not_yet_disbursed_fyb": "ussgl490800_authority_outl_fyb",
    "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": "ussgl490800_authority_outl_cpe",
    "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": "ussgl498200_upward_adjustm_cpe",
    "gross_outlays_delivered_orders_paid_total_fyb": "gross_outlays_delivered_or_fyb",
    "gross_outlays_delivered_orders_paid_total_cpe": "gross_outlays_delivered_or_cpe",
    "gross_outlay_amount_by_award_fyb": "gross_outlay_amount_by_awa_fyb",
    "gross_outlay_amount_by_award_cpe": "gross_outlay_amount_by_awa_cpe",
    "obligations_incurred_total_by_award_cpe": "obligations_incurred_byawa_cpe",
    "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": "ussgl487100_downward_adjus_cpe",
    "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": "ussgl497100_downward_adjus_cpe",
    "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": "ussgl487200_downward_adjus_cpe",
    "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": "ussgl497200_downward_adjus_cpe",
    "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": "ussgl498100_upward_adjustm_cpe",
    "deobligations_recoveries_refunds_of_prior_year_by_award_cpe": "deobligations_recov_by_awa_cpe",
    "gross_outlay_amount_by_program_object_class_fyb": "gross_outlay_amount_by_pro_fyb",
    "gross_outlay_amount_by_program_object_class_cpe": "gross_outlay_amount_by_pro_cpe",
    "obligations_incurred_by_program_object_class_cpe": "obligations_incurred_by_pr_cpe",
    "deobligations_recoveries_refund_pri_program_object_class_cpe": "deobligations_recov_by_pro_cpe",
    "sub_recipient_name": "sub_awardee_or_recipient_l",
    "sub_recipient_unique_id": "sub_awardee_or_recipient_u",
    "sub_recipient_ultimate_parent_unique_id": "sub_awardee_ultimate_pa_id",
    "sub_recipient_ultimate_parent_name": "sub_awardee_ultimate_paren",
    "recipient_foreign_postal_code": "legal_entity_foreign_posta",
    "high_comp_officer_full_name": "high_comp_officer_full_nam",
    "high_comp_officer_amount": "high_comp_officer_amount",
    "subcontract_award_amount": "subcontract_award_amount",
    "total_funding_amount": "total_funding_amount",
    "cfda_number_and_title": "cfda_number_and_title",
    "primary_place_of_performance_city_name": "place_of_performance_city",
    "primary_place_of_performance_address_line1": "place_of_performance_addre",
    "primary_place_of_performance_zip4": "place_of_performance_zip4",
    "primary_place_of_performance_country_name": "place_of_perform_country_n",
    "prime_award_report_id": "prime_award_report_id",
    "award_report_month": "award_report_month",
    "award_report_year": "award_report_year",
    "rec_model_question1": "rec_model_question1",
    "rec_model_question2": "rec_model_question2",
    "subaward_number": "subaward_number",
    "subawardee_business_type": "subawardee_business_type",
    "high_comp_officer_first_name": "high_comp_officer_first_na",
    "high_comp_officer_middle_initial": "high_comp_officer_middle_i",
    "high_comp_officer_last_name": "high_comp_officer_last_nam",
    "assistance_type": "assistance_type",
    "assistance_type_description": "assistance_type_desc",
    "record_type": "record_type",
    "record_type_description": "record_type_description",
    "correction_delete_indicator": "correction_delete_indicatr",
    "correction_delete_indicator_description": "correction_delete_ind_desc",
    "fiscal_year_and_quarter_correction": "fiscal_year_and_quarter_co",
    "sai_number": "sai_number",
    "recipient_city_code": "legal_entity_city_code",
    "recipient_county_name": "legal_entity_county_name",
    "recipient_county_code": "legal_entity_county_code",
    "recipient_state_name": "legal_entity_state_name",
    "recipient_zip5": "legal_entity_zip5",
    "recipient_zip_last4": "legal_entity_zip_last4",
    "recipient_foreign_city_name": "legal_entity_foreign_city",
    "recipient_foreign_province_name": "legal_entity_foreign_provi",
    "business_types": "business_types",
    "business_types_desc": "business_types_desc",
    "cfda_number": "cfda_number",
    "cfda_title": "cfda_title",
    "primary_place_of_performance_code": "place_of_performance_code",
    "primary_place_of_performance_state_name": "place_of_perform_state_nam",
    "primary_place_of_performance_county_name": "place_of_perform_county_na",
    "primary_place_of_performance_foreign_location_description": "place_of_performance_forei",
    "non_federal_funding_amount": "non_federal_funding_amount",
    "face_value_loan_guarantee": "face_value_loan_guarantee",
    "original_loan_subsidy_cost": "original_loan_subsidy_cost",
    "business_funds_indicator": "business_funds_indicator",
    "business_funds_ind_desc": "business_funds_ind_desc",
    "indirect_federal_sharing": "indirect_federal_sharing",
}
