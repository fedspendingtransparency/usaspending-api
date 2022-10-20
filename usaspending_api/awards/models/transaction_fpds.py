from django.db import models


class TransactionFPDS(models.Model):
    transaction = models.OneToOneField(
        "awards.TransactionNormalized",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="contract_data",
        help_text="Non-specific transaction data, fields shared among both assistance and contract transactions",
    )
    detached_award_procurement_id = models.IntegerField(blank=True, null=True, db_index=True)
    detached_award_proc_unique = models.TextField(unique=True, null=True)
    piid = models.TextField(blank=True, null=True, db_index=True)
    agency_id = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True, db_index=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    type_of_contract_pricing = models.TextField(blank=True, null=True, db_index=True)
    type_of_contract_pric_desc = models.TextField(blank=True, null=True)
    contract_award_type = models.TextField(blank=True, null=True)
    contract_award_type_desc = models.TextField(blank=True, null=True)
    naics = models.TextField(blank=True, null=True, db_index=True)
    naics_description = models.TextField(blank=True, null=True)
    awardee_or_recipient_uei = models.TextField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)
    ultimate_parent_uei = models.TextField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    place_of_performance_zip4a = models.TextField(blank=True, null=True)
    place_of_performance_zip5 = models.TextField(blank=True, null=True)
    place_of_perform_zip_last4 = models.TextField(blank=True, null=True)
    place_of_perform_city_name = models.TextField(blank=True, null=True)
    place_of_perform_county_na = models.TextField(blank=True, null=True)
    place_of_perform_county_co = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_descrip = models.TextField(blank=True, null=True)
    legal_entity_county_code = models.TextField(blank=True, null=True)
    legal_entity_county_name = models.TextField(blank=True, null=True)
    legal_entity_zip4 = models.TextField(blank=True, null=True)
    legal_entity_zip5 = models.TextField(blank=True, null=True)
    legal_entity_zip_last4 = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    period_of_performance_star = models.TextField(blank=True, null=True)
    period_of_performance_curr = models.TextField(blank=True, null=True)
    period_of_perf_potential_e = models.TextField(blank=True, null=True)
    ordering_period_end_date = models.TextField(blank=True, null=True)
    action_date = models.TextField(blank=True, null=True)
    action_type = models.TextField(blank=True, null=True)
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    current_total_value_award = models.TextField(blank=True, null=True)
    potential_total_value_awar = models.TextField(blank=True, null=True)
    total_obligated_amount = models.TextField(blank=True, null=True)
    base_exercised_options_val = models.TextField(blank=True, null=True)
    base_and_all_options_value = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    referenced_idv_agency_iden = models.TextField(blank=True, null=True)
    referenced_idv_agency_desc = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    place_of_performance_locat = models.TextField(blank=True, null=True)
    place_of_performance_state = models.TextField(blank=True, null=True)
    place_of_perfor_state_desc = models.TextField(blank=True, null=True)
    place_of_perform_country_c = models.TextField(blank=True, null=True)
    place_of_perf_country_desc = models.TextField(blank=True, null=True)
    idv_type = models.TextField(blank=True, null=True)
    idv_type_description = models.TextField(blank=True, null=True)
    award_or_idv_flag = models.TextField(blank=True, null=True)
    referenced_idv_type = models.TextField(blank=True, null=True)
    referenced_idv_type_desc = models.TextField(blank=True, null=True)
    vendor_doing_as_business_n = models.TextField(blank=True, null=True)
    vendor_phone_number = models.TextField(blank=True, null=True)
    vendor_fax_number = models.TextField(blank=True, null=True)
    multiple_or_single_award_i = models.TextField(blank=True, null=True)
    multiple_or_single_aw_desc = models.TextField(blank=True, null=True)
    referenced_mult_or_single = models.TextField(blank=True, null=True)
    referenced_mult_or_si_desc = models.TextField(blank=True, null=True)
    type_of_idc = models.TextField(blank=True, null=True)
    type_of_idc_description = models.TextField(blank=True, null=True)
    a_76_fair_act_action = models.TextField(blank=True, null=True)
    a_76_fair_act_action_desc = models.TextField(blank=True, null=True)
    dod_claimant_program_code = models.TextField(blank=True, null=True)
    dod_claimant_prog_cod_desc = models.TextField(blank=True, null=True)
    clinger_cohen_act_planning = models.TextField(blank=True, null=True)
    clinger_cohen_act_pla_desc = models.TextField(blank=True, null=True)
    commercial_item_acquisitio = models.TextField(blank=True, null=True)
    commercial_item_acqui_desc = models.TextField(blank=True, null=True)
    commercial_item_test_progr = models.TextField(blank=True, null=True)
    commercial_item_test_desc = models.TextField(blank=True, null=True)
    consolidated_contract = models.TextField(blank=True, null=True)
    consolidated_contract_desc = models.TextField(blank=True, null=True)
    contingency_humanitarian_o = models.TextField(blank=True, null=True)
    contingency_humanitar_desc = models.TextField(blank=True, null=True)
    contract_bundling = models.TextField(blank=True, null=True)
    contract_bundling_descrip = models.TextField(blank=True, null=True)
    contract_financing = models.TextField(blank=True, null=True)
    contract_financing_descrip = models.TextField(blank=True, null=True)
    contracting_officers_deter = models.TextField(blank=True, null=True)
    contracting_officers_desc = models.TextField(blank=True, null=True)
    cost_accounting_standards = models.TextField(blank=True, null=True)
    cost_accounting_stand_desc = models.TextField(blank=True, null=True)
    cost_or_pricing_data = models.TextField(blank=True, null=True)
    cost_or_pricing_data_desc = models.TextField(blank=True, null=True)
    country_of_product_or_serv = models.TextField(blank=True, null=True)
    country_of_product_or_desc = models.TextField(blank=True, null=True)
    construction_wage_rate_req = models.TextField(blank=True, null=True)
    construction_wage_rat_desc = models.TextField(blank=True, null=True)
    evaluated_preference = models.TextField(blank=True, null=True)
    evaluated_preference_desc = models.TextField(blank=True, null=True)
    extent_competed = models.TextField(blank=True, null=True, db_index=True)
    extent_compete_description = models.TextField(blank=True, null=True)
    fed_biz_opps = models.TextField(blank=True, null=True)
    fed_biz_opps_description = models.TextField(blank=True, null=True)
    foreign_funding = models.TextField(blank=True, null=True)
    foreign_funding_desc = models.TextField(blank=True, null=True)
    government_furnished_prope = models.TextField(blank=True, null=True)
    government_furnished_desc = models.TextField(blank=True, null=True)
    information_technology_com = models.TextField(blank=True, null=True)
    information_technolog_desc = models.TextField(blank=True, null=True)
    interagency_contracting_au = models.TextField(blank=True, null=True)
    interagency_contract_desc = models.TextField(blank=True, null=True)
    local_area_set_aside = models.TextField(blank=True, null=True)
    local_area_set_aside_desc = models.TextField(blank=True, null=True)
    major_program = models.TextField(blank=True, null=True)
    purchase_card_as_payment_m = models.TextField(blank=True, null=True)
    purchase_card_as_paym_desc = models.TextField(blank=True, null=True)
    multi_year_contract = models.TextField(blank=True, null=True)
    multi_year_contract_desc = models.TextField(blank=True, null=True)
    national_interest_action = models.TextField(blank=True, null=True)
    national_interest_desc = models.TextField(blank=True, null=True)
    number_of_actions = models.TextField(blank=True, null=True)
    number_of_offers_received = models.TextField(blank=True, null=True)
    other_statutory_authority = models.TextField(blank=True, null=True)
    performance_based_service = models.TextField(blank=True, null=True)
    performance_based_se_desc = models.TextField(blank=True, null=True)
    place_of_manufacture = models.TextField(blank=True, null=True)
    place_of_manufacture_desc = models.TextField(blank=True, null=True)
    price_evaluation_adjustmen = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField(blank=True, null=True, db_index=True)
    product_or_service_co_desc = models.TextField(blank=True, null=True)
    program_acronym = models.TextField(blank=True, null=True)
    other_than_full_and_open_c = models.TextField(blank=True, null=True)
    other_than_full_and_o_desc = models.TextField(blank=True, null=True)
    recovered_materials_sustai = models.TextField(blank=True, null=True)
    recovered_materials_s_desc = models.TextField(blank=True, null=True)
    research = models.TextField(blank=True, null=True)
    research_description = models.TextField(blank=True, null=True)
    sea_transportation = models.TextField(blank=True, null=True)
    sea_transportation_desc = models.TextField(blank=True, null=True)
    labor_standards = models.TextField(blank=True, null=True)
    labor_standards_descrip = models.TextField(blank=True, null=True)
    small_business_competitive = models.BooleanField(null=True, blank=True)
    solicitation_identifier = models.TextField(blank=True, null=True)
    solicitation_procedures = models.TextField(blank=True, null=True)
    solicitation_procedur_desc = models.TextField(blank=True, null=True)
    fair_opportunity_limited_s = models.TextField(blank=True, null=True)
    fair_opportunity_limi_desc = models.TextField(blank=True, null=True)
    subcontracting_plan = models.TextField(blank=True, null=True)
    subcontracting_plan_desc = models.TextField(blank=True, null=True)
    program_system_or_equipmen = models.TextField(blank=True, null=True)
    program_system_or_equ_desc = models.TextField(blank=True, null=True)
    type_set_aside = models.TextField(blank=True, null=True, db_index=True)
    type_set_aside_description = models.TextField(blank=True, null=True)
    epa_designated_product = models.TextField(blank=True, null=True)
    epa_designated_produc_desc = models.TextField(blank=True, null=True)
    materials_supplies_article = models.TextField(blank=True, null=True)
    materials_supplies_descrip = models.TextField(blank=True, null=True)
    transaction_number = models.TextField(blank=True, null=True)
    sam_exception = models.TextField(blank=True, null=True)
    sam_exception_description = models.TextField(blank=True, null=True)
    city_local_government = models.BooleanField(null=True, blank=True)
    county_local_government = models.BooleanField(null=True, blank=True)
    inter_municipal_local_gove = models.BooleanField(null=True, blank=True)
    local_government_owned = models.BooleanField(null=True, blank=True)
    municipality_local_governm = models.BooleanField(null=True, blank=True)
    school_district_local_gove = models.BooleanField(null=True, blank=True)
    township_local_government = models.BooleanField(null=True, blank=True)
    us_state_government = models.BooleanField(null=True, blank=True)
    us_federal_government = models.BooleanField(null=True, blank=True)
    federal_agency = models.BooleanField(null=True, blank=True)
    federally_funded_research = models.BooleanField(null=True, blank=True)
    us_tribal_government = models.BooleanField(null=True, blank=True)
    foreign_government = models.BooleanField(null=True, blank=True)
    community_developed_corpor = models.BooleanField(null=True, blank=True)
    labor_surplus_area_firm = models.BooleanField(null=True, blank=True)
    corporate_entity_not_tax_e = models.BooleanField(null=True, blank=True)
    corporate_entity_tax_exemp = models.BooleanField(null=True, blank=True)
    partnership_or_limited_lia = models.BooleanField(null=True, blank=True)
    sole_proprietorship = models.BooleanField(null=True, blank=True)
    small_agricultural_coopera = models.BooleanField(null=True, blank=True)
    international_organization = models.BooleanField(null=True, blank=True)
    us_government_entity = models.BooleanField(null=True, blank=True)
    emerging_small_business = models.BooleanField(null=True, blank=True)
    c8a_program_participant = models.BooleanField(null=True, blank=True)
    sba_certified_8_a_joint_ve = models.BooleanField(null=True, blank=True)
    dot_certified_disadvantage = models.BooleanField(null=True, blank=True)
    self_certified_small_disad = models.BooleanField(null=True, blank=True)
    historically_underutilized = models.BooleanField(null=True, blank=True)
    small_disadvantaged_busine = models.BooleanField(null=True, blank=True)
    the_ability_one_program = models.BooleanField(null=True, blank=True)
    historically_black_college = models.BooleanField(null=True, blank=True)
    c1862_land_grant_college = models.BooleanField(null=True, blank=True)
    c1890_land_grant_college = models.BooleanField(null=True, blank=True)
    c1994_land_grant_college = models.BooleanField(null=True, blank=True)
    minority_institution = models.BooleanField(null=True, blank=True)
    private_university_or_coll = models.BooleanField(null=True, blank=True)
    school_of_forestry = models.BooleanField(null=True, blank=True)
    state_controlled_instituti = models.BooleanField(null=True, blank=True)
    tribal_college = models.BooleanField(null=True, blank=True)
    veterinary_college = models.BooleanField(null=True, blank=True)
    educational_institution = models.BooleanField(null=True, blank=True)
    alaskan_native_servicing_i = models.BooleanField(null=True, blank=True)
    community_development_corp = models.BooleanField(null=True, blank=True)
    native_hawaiian_servicing = models.BooleanField(null=True, blank=True)
    domestic_shelter = models.BooleanField(null=True, blank=True)
    manufacturer_of_goods = models.BooleanField(null=True, blank=True)
    hospital_flag = models.BooleanField(null=True, blank=True)
    veterinary_hospital = models.BooleanField(null=True, blank=True)
    hispanic_servicing_institu = models.BooleanField(null=True, blank=True)
    foundation = models.BooleanField(null=True, blank=True)
    woman_owned_business = models.BooleanField(null=True, blank=True)
    minority_owned_business = models.BooleanField(null=True, blank=True)
    women_owned_small_business = models.BooleanField(null=True, blank=True)
    economically_disadvantaged = models.BooleanField(null=True, blank=True)
    joint_venture_women_owned = models.BooleanField(null=True, blank=True)
    joint_venture_economically = models.BooleanField(null=True, blank=True)
    veteran_owned_business = models.BooleanField(null=True, blank=True)
    service_disabled_veteran_o = models.BooleanField(null=True, blank=True)
    contracts = models.BooleanField(null=True, blank=True)
    grants = models.BooleanField(null=True, blank=True)
    receives_contracts_and_gra = models.BooleanField(null=True, blank=True)
    airport_authority = models.BooleanField(null=True, blank=True)
    council_of_governments = models.BooleanField(null=True, blank=True)
    housing_authorities_public = models.BooleanField(null=True, blank=True)
    interstate_entity = models.BooleanField(null=True, blank=True)
    planning_commission = models.BooleanField(null=True, blank=True)
    port_authority = models.BooleanField(null=True, blank=True)
    transit_authority = models.BooleanField(null=True, blank=True)
    subchapter_s_corporation = models.BooleanField(null=True, blank=True)
    limited_liability_corporat = models.BooleanField(null=True, blank=True)
    foreign_owned_and_located = models.BooleanField(null=True, blank=True)
    american_indian_owned_busi = models.BooleanField(null=True, blank=True)
    alaskan_native_owned_corpo = models.BooleanField(null=True, blank=True)
    indian_tribe_federally_rec = models.BooleanField(null=True, blank=True)
    native_hawaiian_owned_busi = models.BooleanField(null=True, blank=True)
    tribally_owned_business = models.BooleanField(null=True, blank=True)
    asian_pacific_american_own = models.BooleanField(null=True, blank=True)
    black_american_owned_busin = models.BooleanField(null=True, blank=True)
    hispanic_american_owned_bu = models.BooleanField(null=True, blank=True)
    native_american_owned_busi = models.BooleanField(null=True, blank=True)
    subcontinent_asian_asian_i = models.BooleanField(null=True, blank=True)
    other_minority_owned_busin = models.BooleanField(null=True, blank=True)
    for_profit_organization = models.BooleanField(null=True, blank=True)
    nonprofit_organization = models.BooleanField(null=True, blank=True)
    other_not_for_profit_organ = models.BooleanField(null=True, blank=True)
    us_local_government = models.BooleanField(null=True, blank=True)
    referenced_idv_modificatio = models.TextField(blank=True, null=True)
    undefinitized_action = models.TextField(blank=True, null=True)
    undefinitized_action_desc = models.TextField(blank=True, null=True)
    domestic_or_foreign_entity = models.TextField(blank=True, null=True)
    domestic_or_foreign_e_desc = models.TextField(blank=True, null=True)
    annual_revenue = models.TextField(blank=True, null=True)
    division_name = models.TextField(blank=True, null=True)
    division_number_or_office = models.TextField(blank=True, null=True)
    number_of_employees = models.TextField(blank=True, null=True)
    vendor_alternate_name = models.TextField(blank=True, null=True)
    vendor_alternate_site_code = models.TextField(blank=True, null=True)
    vendor_enabled = models.TextField(blank=True, null=True)
    vendor_legal_org_name = models.TextField(blank=True, null=True)
    vendor_location_disabled_f = models.TextField(blank=True, null=True)
    vendor_site_code = models.TextField(blank=True, null=True)
    pulled_from = models.TextField(blank=True, null=True)
    last_modified = models.TextField(blank=True, null=True)
    initial_report_date = models.TextField(blank=True, null=True)
    cage_code = models.TextField(blank=True, null=True)
    inherently_government_func = models.TextField(blank=True, null=True)
    inherently_government_desc = models.TextField(blank=True, null=True)
    organizational_type = models.TextField(blank=True, null=True)
    referenced_idv_agency_name = models.TextField(blank=True, null=True)
    referenced_multi_or_single = models.TextField(blank=True, null=True)
    place_of_perform_country_n = models.TextField(blank=True, null=True)
    place_of_perform_state_nam = models.TextField(blank=True, null=True)
    unique_award_key = models.TextField(null=True, db_index=True)
    solicitation_date = models.DateField(null=True, blank=True)
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    entity_data_source = models.TextField(blank=True, null=True)

    # Timestamp field auto generated by broker
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True, db_index=True)

    class Meta:
        managed = False
        db_table = "vw_transaction_fpds"


vw_transaction_fpds_sql = """
    CREATE OR REPLACE VIEW rpt.vw_transaction_fpds AS
        SELECT
            -- Keys
            transaction_id,
            modification_number,
            generated_unique_award_id               AS "unique_award_key",
            -- Dates
            action_date,
            last_modified_date                      AS "last_modified",
            period_of_performance_start_date        AS "period_of_performance_star",
            period_of_performance_current_end_date  AS "period_of_performance_curr",
            -- Agencies
            awarding_agency_code,
            awarding_toptier_agency_name            AS "awarding_agency_name",
            funding_agency_code,
            funding_toptier_agency_name             AS "funding_agency_name",
            awarding_sub_tier_agency_c,
            awarding_subtier_agency_name            AS "awarding_sub_tier_agency_n",
            funding_sub_tier_agency_co,
            funding_subtier_agency_name             AS "funding_sub_tier_agency_na",
            awarding_office_code,
            awarding_office_name,
            funding_office_code,
            funding_office_name,
            -- Typing
            action_type,
            action_type_description,
            transaction_description                 AS "award_description",
            -- Amounts
            federal_action_obligation,
            -- Recipient
            recipient_uei                           AS "awardee_or_recipient_uei",
            recipient_name_raw                      AS "awardee_or_recipient_legal",
            recipient_unique_id                     AS "awardee_or_recipient_uniqu",
            parent_uei                              AS "ultimate_parent_uei",
            parent_recipient_name                   AS "ultimate_parent_legal_enti",
            parent_recipient_unique_id              AS "ultimate_parent_unique_ide",
            -- Recipient Location
            recipient_location_country_code         AS "legal_entity_country_code",
            recipient_location_country_name         AS "legal_entity_country_name",
            recipient_location_state_code           AS "legal_entity_state_code",
            recipient_location_state_name           AS "legal_entity_state_descrip",
            recipient_location_county_code          AS "legal_entity_county_code",
            recipient_location_county_name          AS "legal_entity_county_name",
            recipient_location_congressional_code   AS "legal_entity_congressional",
            recipient_location_zip5                 AS "legal_entity_zip5",
            legal_entity_zip4,
            legal_entity_zip_last4,
            recipient_location_city_name            AS "legal_entity_city_name",
            legal_entity_address_line1,
            legal_entity_address_line2,
            legal_entity_address_line3,
            -- Place of Performance
            pop_country_code                        AS "place_of_perform_country_c",
            pop_country_name                        AS "place_of_perf_country_desc",
            NULL                                    AS "place_of_perform_country_n",
            pop_state_code                          AS "place_of_performance_state",
            pop_state_name                          AS "place_of_perfor_state_desc",
            NULL                                    AS "place_of_perform_state_nam",
            pop_county_code                         AS "place_of_perform_county_co",
            pop_county_name                         AS "place_of_perform_county_na",
            pop_congressional_code                  AS "place_of_performance_congr",
            pop_zip5                                AS "place_of_performance_zip5",
            place_of_performance_zip4a,
            place_of_perform_zip_last4,
            pop_city_name                           AS "place_of_perform_city_name",
            -- Officer Amounts
            officer_1_name,
            officer_1_amount,
            officer_2_name,
            officer_2_amount,
            officer_3_name,
            officer_3_amount,
            officer_4_name,
            officer_4_amount,
            officer_5_name,
            officer_5_amount,
            -- Exclusively FPDS
            detached_award_proc_unique,
            a_76_fair_act_action,
            a_76_fair_act_action_desc,
            agency_id,
            airport_authority,
            alaskan_native_owned_corpo,
            alaskan_native_servicing_i,
            american_indian_owned_busi,
            asian_pacific_american_own,
            base_and_all_options_value,
            base_exercised_options_val,
            black_american_owned_busin,
            c1862_land_grant_college,
            c1890_land_grant_college,
            c1994_land_grant_college,
            c8a_program_participant,
            cage_code,
            city_local_government,
            clinger_cohen_act_pla_desc,
            clinger_cohen_act_planning,
            commercial_item_acqui_desc,
            commercial_item_acquisitio,
            commercial_item_test_desc,
            commercial_item_test_progr,
            community_developed_corpor,
            community_development_corp,
            consolidated_contract,
            consolidated_contract_desc,
            construction_wage_rat_desc,
            construction_wage_rate_req,
            contingency_humanitar_desc,
            contingency_humanitarian_o,
            contract_award_type,
            contract_award_type_desc,
            contract_bundling,
            contract_bundling_descrip,
            contract_financing,
            contract_financing_descrip,
            contracting_officers_desc,
            contracting_officers_deter,
            contracts,
            corporate_entity_not_tax_e,
            corporate_entity_tax_exemp,
            cost_accounting_stand_desc,
            cost_accounting_standards,
            cost_or_pricing_data,
            cost_or_pricing_data_desc,
            council_of_governments,
            country_of_product_or_desc,
            country_of_product_or_serv,
            county_local_government,
            current_total_value_award,
            dod_claimant_prog_cod_desc,
            dod_claimant_program_code,
            domestic_or_foreign_e_desc,
            domestic_or_foreign_entity,
            domestic_shelter,
            dot_certified_disadvantage,
            economically_disadvantaged,
            educational_institution,
            emerging_small_business,
            epa_designated_produc_desc,
            epa_designated_product,
            evaluated_preference,
            evaluated_preference_desc,
            extent_compete_description,
            extent_competed,
            fair_opportunity_limi_desc,
            fair_opportunity_limited_s,
            fed_biz_opps,
            fed_biz_opps_description,
            federal_agency,
            federally_funded_research,
            for_profit_organization,
            foreign_funding,
            foreign_funding_desc,
            foreign_government,
            foreign_owned_and_located,
            foundation,
            government_furnished_desc,
            government_furnished_prope,
            grants,
            hispanic_american_owned_bu,
            hispanic_servicing_institu,
            historically_black_college,
            historically_underutilized,
            hospital_flag,
            housing_authorities_public,
            idv_type,
            idv_type_description,
            indian_tribe_federally_rec,
            information_technolog_desc,
            information_technology_com,
            inherently_government_desc,
            inherently_government_func,
            inter_municipal_local_gove,
            interagency_contract_desc,
            interagency_contracting_au,
            international_organization,
            interstate_entity,
            joint_venture_economically,
            joint_venture_women_owned,
            labor_standards,
            labor_standards_descrip,
            labor_surplus_area_firm,
            limited_liability_corporat,
            local_area_set_aside,
            local_area_set_aside_desc,
            local_government_owned,
            major_program,
            manufacturer_of_goods,
            materials_supplies_article,
            materials_supplies_descrip,
            minority_institution,
            minority_owned_business,
            multi_year_contract,
            multi_year_contract_desc,
            multiple_or_single_aw_desc,
            multiple_or_single_award_i,
            municipality_local_governm,
            naics_code                              AS "naics",
            naics_description,
            national_interest_action,
            national_interest_desc,
            native_american_owned_busi,
            native_hawaiian_owned_busi,
            native_hawaiian_servicing,
            nonprofit_organization,
            number_of_actions,
            number_of_offers_received,
            ordering_period_end_date,
            organizational_type,
            other_minority_owned_busin,
            other_not_for_profit_organ,
            other_statutory_authority,
            other_than_full_and_o_desc,
            other_than_full_and_open_c,
            parent_award_id,
            partnership_or_limited_lia,
            performance_based_se_desc,
            performance_based_service,
            period_of_perf_potential_e,
            piid,
            place_of_manufacture,
            place_of_manufacture_desc,
            planning_commission,
            port_authority,
            potential_total_value_awar,
            price_evaluation_adjustmen,
            private_university_or_coll,
            product_or_service_code,
            product_or_service_description          AS "product_or_service_co_desc",
            program_acronym,
            program_system_or_equ_desc,
            program_system_or_equipmen,
            pulled_from,
            purchase_card_as_paym_desc,
            purchase_card_as_payment_m,
            receives_contracts_and_gra,
            recovered_materials_s_desc,
            recovered_materials_sustai,
            referenced_idv_agency_desc,
            referenced_idv_agency_iden,
            referenced_idv_modificatio,
            referenced_idv_type,
            referenced_idv_type_desc,
            referenced_mult_or_si_desc,
            referenced_mult_or_single,
            research,
            research_description,
            sam_exception,
            sam_exception_description,
            sba_certified_8_a_joint_ve,
            school_district_local_gove,
            school_of_forestry,
            sea_transportation,
            sea_transportation_desc,
            self_certified_small_disad,
            service_disabled_veteran_o,
            small_agricultural_coopera,
            small_business_competitive,
            small_disadvantaged_busine,
            sole_proprietorship,
            solicitation_date,
            solicitation_identifier,
            solicitation_procedur_desc,
            solicitation_procedures,
            state_controlled_instituti,
            subchapter_s_corporation,
            subcontinent_asian_asian_i,
            subcontracting_plan,
            subcontracting_plan_desc,
            the_ability_one_program,
            total_obligated_amount,
            township_local_government,
            transaction_number,
            transit_authority,
            tribal_college,
            tribally_owned_business,
            type_of_contract_pric_desc,
            type_of_contract_pricing,
            type_of_idc,
            type_of_idc_description,
            type_set_aside,
            type_set_aside_description,
            undefinitized_action,
            undefinitized_action_desc,
            us_federal_government,
            us_government_entity,
            us_local_government,
            us_state_government,
            us_tribal_government,
            vendor_doing_as_business_n,
            vendor_fax_number,
            vendor_phone_number,
            veteran_owned_business,
            veterinary_college,
            veterinary_hospital,
            woman_owned_business,
            women_owned_small_business
        FROM
            rpt.transaction_search
        WHERE
            is_fpds = True;
"""
