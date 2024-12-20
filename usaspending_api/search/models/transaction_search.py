from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models import Q

from usaspending_api.common.custom_django_fields import NumericField


class TransactionSearch(models.Model):
    """
    Fields in this model have all, with the exception of primary/foreign keys, been made nullable because it
    is directly populated by the contents of a materialized view. The fields used to create the materialized view
    may or may not be nullable, but those constraints are not enforced in this table.
    """

    # Keys
    # "transaction" and "award" are actually models.BigIntegerField(), but left as OneToOneField and ForeignKey
    # to allow for querying in the Django ORM
    # Also, this table has been physically partitioned by partition key: is_fpds. We can no longer have a UNIQUE key
    # or UNIQUE INDEX on transaction_id (the primary_key) anymore, it must include the partition key. So setting
    # primary_key=False and adding a UniqueConstraint (is_fpds, transaction)
    transaction = models.OneToOneField("awards.TransactionNormalized", on_delete=models.DO_NOTHING, primary_key=True)
    award = models.ForeignKey("search.AwardSearch", on_delete=models.DO_NOTHING, null=True)
    transaction_unique_id = models.TextField(blank=False, null=False, default="NONE")
    usaspending_unique_transaction_id = models.TextField(null=True)
    modification_number = models.TextField(null=True)
    generated_unique_award_id = models.TextField(null=True)

    # Dates
    action_date = models.DateField(null=True)
    fiscal_action_date = models.DateField(null=True)
    last_modified_date = models.DateField(null=True)
    fiscal_year = models.IntegerField(null=True)
    award_certified_date = models.DateField(null=True)
    award_fiscal_year = models.IntegerField(null=True)
    create_date = models.DateTimeField(null=True)
    update_date = models.DateTimeField(null=True)
    award_update_date = models.DateTimeField(null=True)
    award_date_signed = models.DateField(null=True)
    etl_update_date = models.DateTimeField(null=True)
    period_of_performance_start_date = models.DateField(null=True)
    period_of_performance_current_end_date = models.DateField(null=True)
    initial_report_date = models.DateField(null=True)

    # Agencies
    awarding_agency_code = models.TextField(null=True)
    awarding_toptier_agency_name = models.TextField(null=True)
    awarding_toptier_agency_name_raw = models.TextField(null=True)
    funding_agency_code = models.TextField(null=True)
    funding_toptier_agency_name = models.TextField(null=True)
    funding_toptier_agency_name_raw = models.TextField(null=True)
    awarding_sub_tier_agency_c = models.TextField(null=True)
    awarding_subtier_agency_name = models.TextField(null=True)
    awarding_subtier_agency_name_raw = models.TextField(null=True)
    funding_sub_tier_agency_co = models.TextField(null=True)
    funding_subtier_agency_name = models.TextField(null=True)
    funding_subtier_agency_name_raw = models.TextField(null=True)
    awarding_toptier_agency_id = models.IntegerField(null=True)
    funding_toptier_agency_id = models.IntegerField(null=True)
    awarding_agency_id = models.IntegerField(null=True)
    funding_agency_id = models.IntegerField(null=True)
    awarding_toptier_agency_abbreviation = models.TextField(null=True)
    funding_toptier_agency_abbreviation = models.TextField(null=True)
    awarding_subtier_agency_abbreviation = models.TextField(null=True)
    funding_subtier_agency_abbreviation = models.TextField(null=True)
    awarding_office_code = models.TextField(null=True)
    awarding_office_name = models.TextField(null=True)
    funding_office_code = models.TextField(null=True)
    funding_office_name = models.TextField(null=True)

    # Typing
    is_fpds = models.BooleanField(blank=False, null=False)
    type_raw = models.TextField(null=True)
    type_description_raw = models.TextField(null=True)
    type = models.TextField(null=True)
    type_description = models.TextField(null=True)
    action_type = models.TextField(null=True)
    action_type_description = models.TextField(null=True)
    award_category = models.TextField(null=True)
    transaction_description = models.TextField(null=True)
    business_categories = ArrayField(models.TextField(), null=True)

    # Amounts
    award_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    indirect_federal_sharing = NumericField(blank=True, null=True)
    funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    # Recipient
    recipient_hash = models.UUIDField(null=True)
    recipient_levels = ArrayField(models.TextField(), null=True)
    recipient_uei = models.TextField(null=True)
    recipient_name_raw = models.TextField(null=True)
    recipient_name = models.TextField(null=True)
    recipient_unique_id = models.TextField(null=True)
    parent_recipient_hash = models.UUIDField(null=True)
    parent_uei = models.TextField(null=True)
    parent_recipient_name_raw = models.TextField(null=True)
    parent_recipient_name = models.TextField(null=True)
    parent_recipient_unique_id = models.TextField(null=True)

    # Recipient Location
    recipient_location_country_code = models.TextField(null=True)
    recipient_location_country_name = models.TextField(null=True)
    recipient_location_state_code = models.TextField(null=True)
    recipient_location_state_name = models.TextField(null=True)
    recipient_location_state_fips = models.TextField(null=True)
    recipient_location_state_population = models.IntegerField(null=True)
    recipient_location_county_code = models.TextField(null=True)
    recipient_location_county_name = models.TextField(null=True)
    recipient_location_county_population = models.IntegerField(null=True)
    recipient_location_congressional_code = models.TextField(null=True)
    recipient_location_congressional_population = models.IntegerField(null=True)
    recipient_location_congressional_code_current = models.TextField(null=True)
    recipient_location_zip5 = models.TextField(null=True)
    legal_entity_zip4 = models.TextField(null=True)
    legal_entity_zip_last4 = models.TextField(null=True)
    legal_entity_city_code = models.TextField(null=True)
    recipient_location_city_name = models.TextField(null=True)
    legal_entity_address_line1 = models.TextField(null=True)
    legal_entity_address_line2 = models.TextField(null=True)
    legal_entity_address_line3 = models.TextField(null=True)
    legal_entity_foreign_city = models.TextField(null=True)
    legal_entity_foreign_descr = models.TextField(null=True)
    legal_entity_foreign_posta = models.TextField(null=True)
    legal_entity_foreign_provi = models.TextField(null=True)
    recipient_location_county_fips = models.TextField(null=True)

    # Place of Performance
    place_of_performance_code = models.TextField(null=True)
    place_of_performance_scope = models.TextField(null=True)
    pop_country_code = models.TextField(null=True)
    pop_country_name = models.TextField(null=True)
    pop_state_code = models.TextField(null=True)
    pop_state_name = models.TextField(null=True)
    pop_state_fips = models.TextField(null=True)
    pop_state_population = models.IntegerField(null=True)
    pop_county_code = models.TextField(null=True)
    pop_county_name = models.TextField(null=True)
    pop_county_population = models.IntegerField(null=True)
    pop_congressional_code = models.TextField(null=True)
    pop_congressional_population = models.IntegerField(null=True)
    pop_congressional_code_current = models.TextField(null=True)
    pop_zip5 = models.TextField(null=True)
    place_of_performance_zip4a = models.TextField(null=True)
    place_of_perform_zip_last4 = models.TextField(null=True)
    pop_city_name = models.TextField(null=True)
    place_of_performance_forei = models.TextField(null=True)
    pop_county_fips = models.TextField(null=True)

    # Accounts
    treasury_account_identifiers = ArrayField(models.IntegerField(), null=True)
    tas_paths = ArrayField(models.TextField(), null=True)
    tas_components = ArrayField(models.TextField(), null=True)
    federal_accounts = models.JSONField(null=True)
    disaster_emergency_fund_codes = ArrayField(models.TextField(), null=True)

    # Officer Amounts
    officer_1_name = models.TextField(null=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    # Exclusively FABS
    published_fabs_id = models.IntegerField(blank=True, null=True)
    afa_generated_unique = models.TextField(null=True)
    business_funds_ind_desc = models.TextField(null=True)
    business_funds_indicator = models.TextField(null=True)
    business_types = models.TextField(null=True)
    business_types_desc = models.TextField(null=True)
    cfda_number = models.TextField(null=True)
    cfda_title = models.TextField(null=True)
    cfda_id = models.IntegerField(null=True)
    correction_delete_indicatr = models.TextField(null=True)
    correction_delete_ind_desc = models.TextField(null=True)
    fain = models.TextField(null=True)
    funding_opportunity_goals = models.TextField(blank=True, null=True)
    funding_opportunity_number = models.TextField(blank=True, null=True)
    record_type = models.IntegerField(null=True)
    record_type_description = models.TextField(null=True)
    sai_number = models.TextField(null=True)
    uri = models.TextField(null=True)

    # Exclusively FPDS
    detached_award_procurement_id = models.IntegerField(blank=True, null=True)
    detached_award_proc_unique = models.TextField(null=True)
    a_76_fair_act_action = models.TextField(null=True)
    a_76_fair_act_action_desc = models.TextField(null=True)
    agency_id = models.TextField(null=True)
    airport_authority = models.BooleanField(null=True)
    alaskan_native_owned_corpo = models.BooleanField(null=True)
    alaskan_native_servicing_i = models.BooleanField(null=True)
    american_indian_owned_busi = models.BooleanField(null=True)
    asian_pacific_american_own = models.BooleanField(null=True)
    base_and_all_options_value = models.TextField(null=True)
    base_exercised_options_val = models.TextField(null=True)
    black_american_owned_busin = models.BooleanField(null=True)
    c1862_land_grant_college = models.BooleanField(null=True)
    c1890_land_grant_college = models.BooleanField(null=True)
    c1994_land_grant_college = models.BooleanField(null=True)
    c8a_program_participant = models.BooleanField(null=True)
    cage_code = models.TextField(null=True)
    city_local_government = models.BooleanField(null=True)
    clinger_cohen_act_planning = models.TextField(null=True)
    clinger_cohen_act_pla_desc = models.TextField(null=True)
    commercial_item_acqui_desc = models.TextField(null=True)
    commercial_item_acquisitio = models.TextField(null=True)
    commercial_item_test_desc = models.TextField(null=True)
    commercial_item_test_progr = models.TextField(null=True)
    community_developed_corpor = models.BooleanField(null=True)
    community_development_corp = models.BooleanField(null=True)
    consolidated_contract = models.TextField(null=True)
    consolidated_contract_desc = models.TextField(null=True)
    construction_wage_rat_desc = models.TextField(null=True)
    construction_wage_rate_req = models.TextField(null=True)
    contingency_humanitar_desc = models.TextField(null=True)
    contingency_humanitarian_o = models.TextField(null=True)
    contract_award_type = models.TextField(null=True)
    contract_award_type_desc = models.TextField(null=True)
    contract_bundling = models.TextField(null=True)
    contract_bundling_descrip = models.TextField(null=True)
    contract_financing = models.TextField(null=True)
    contract_financing_descrip = models.TextField(null=True)
    contracting_officers_desc = models.TextField(null=True)
    contracting_officers_deter = models.TextField(null=True)
    contracts = models.BooleanField(null=True)
    corporate_entity_not_tax_e = models.BooleanField(null=True)
    corporate_entity_tax_exemp = models.BooleanField(null=True)
    cost_accounting_stand_desc = models.TextField(null=True)
    cost_accounting_standards = models.TextField(null=True)
    cost_or_pricing_data = models.TextField(null=True)
    cost_or_pricing_data_desc = models.TextField(null=True)
    council_of_governments = models.BooleanField(null=True)
    country_of_product_or_desc = models.TextField(null=True)
    country_of_product_or_serv = models.TextField(null=True)
    county_local_government = models.BooleanField(null=True)
    current_total_value_award = models.TextField(null=True)
    dod_claimant_prog_cod_desc = models.TextField(null=True)
    dod_claimant_program_code = models.TextField(null=True)
    domestic_or_foreign_e_desc = models.TextField(null=True)
    domestic_or_foreign_entity = models.TextField(null=True)
    domestic_shelter = models.BooleanField(null=True)
    dot_certified_disadvantage = models.BooleanField(null=True)
    economically_disadvantaged = models.BooleanField(null=True)
    educational_institution = models.BooleanField(null=True)
    emerging_small_business = models.BooleanField(null=True)
    epa_designated_produc_desc = models.TextField(null=True)
    epa_designated_product = models.TextField(null=True)
    evaluated_preference = models.TextField(null=True)
    evaluated_preference_desc = models.TextField(null=True)
    extent_competed = models.TextField(null=True)
    extent_compete_description = models.TextField(null=True)
    fair_opportunity_limi_desc = models.TextField(null=True)
    fair_opportunity_limited_s = models.TextField(null=True)
    fed_biz_opps = models.TextField(null=True)
    fed_biz_opps_description = models.TextField(null=True)
    federal_agency = models.BooleanField(null=True)
    federally_funded_research = models.BooleanField(null=True)
    for_profit_organization = models.BooleanField(null=True)
    foreign_funding = models.TextField(null=True)
    foreign_funding_desc = models.TextField(null=True)
    foreign_government = models.BooleanField(null=True)
    foreign_owned_and_located = models.BooleanField(null=True)
    foundation = models.BooleanField(null=True)
    government_furnished_desc = models.TextField(null=True)
    government_furnished_prope = models.TextField(null=True)
    grants = models.BooleanField(null=True)
    hispanic_american_owned_bu = models.BooleanField(null=True)
    hispanic_servicing_institu = models.BooleanField(null=True)
    historically_black_college = models.BooleanField(null=True)
    historically_underutilized = models.BooleanField(null=True)
    hospital_flag = models.BooleanField(null=True)
    housing_authorities_public = models.BooleanField(null=True)
    idv_type = models.TextField(null=True)
    idv_type_description = models.TextField(null=True)
    indian_tribe_federally_rec = models.BooleanField(null=True)
    information_technolog_desc = models.TextField(null=True)
    information_technology_com = models.TextField(null=True)
    inherently_government_desc = models.TextField(null=True)
    inherently_government_func = models.TextField(null=True)
    inter_municipal_local_gove = models.BooleanField(null=True)
    interagency_contract_desc = models.TextField(null=True)
    interagency_contracting_au = models.TextField(null=True)
    international_organization = models.BooleanField(null=True)
    interstate_entity = models.BooleanField(null=True)
    joint_venture_economically = models.BooleanField(null=True)
    joint_venture_women_owned = models.BooleanField(null=True)
    labor_standards = models.TextField(null=True)
    labor_standards_descrip = models.TextField(null=True)
    labor_surplus_area_firm = models.BooleanField(null=True)
    limited_liability_corporat = models.BooleanField(null=True)
    local_area_set_aside = models.TextField(null=True)
    local_area_set_aside_desc = models.TextField(null=True)
    local_government_owned = models.BooleanField(null=True)
    major_program = models.TextField(null=True)
    manufacturer_of_goods = models.BooleanField(null=True)
    materials_supplies_article = models.TextField(null=True)
    materials_supplies_descrip = models.TextField(null=True)
    minority_institution = models.BooleanField(null=True)
    minority_owned_business = models.BooleanField(null=True)
    multi_year_contract = models.TextField(null=True)
    multi_year_contract_desc = models.TextField(null=True)
    multiple_or_single_aw_desc = models.TextField(null=True)
    multiple_or_single_award_i = models.TextField(null=True)
    municipality_local_governm = models.BooleanField(null=True)
    naics_code = models.TextField(null=True)
    naics_description = models.TextField(null=True)
    national_interest_action = models.TextField(null=True)
    national_interest_desc = models.TextField(null=True)
    native_american_owned_busi = models.BooleanField(null=True)
    native_hawaiian_owned_busi = models.BooleanField(null=True)
    native_hawaiian_servicing = models.BooleanField(null=True)
    nonprofit_organization = models.BooleanField(null=True)
    number_of_actions = models.TextField(null=True)
    number_of_offers_received = models.TextField(null=True)
    ordering_period_end_date = models.TextField(null=True)
    organizational_type = models.TextField(null=True)
    other_minority_owned_busin = models.BooleanField(null=True)
    other_not_for_profit_organ = models.BooleanField(null=True)
    other_statutory_authority = models.TextField(null=True)
    other_than_full_and_o_desc = models.TextField(null=True)
    other_than_full_and_open_c = models.TextField(null=True)
    parent_award_id = models.TextField(null=True)
    partnership_or_limited_lia = models.BooleanField(null=True)
    performance_based_se_desc = models.TextField(null=True)
    performance_based_service = models.TextField(null=True)
    period_of_perf_potential_e = models.TextField(null=True)
    piid = models.TextField(null=True)
    place_of_manufacture = models.TextField(null=True)
    place_of_manufacture_desc = models.TextField(null=True)
    planning_commission = models.BooleanField(null=True)
    port_authority = models.BooleanField(null=True)
    potential_total_value_awar = models.TextField(null=True)
    price_evaluation_adjustmen = models.TextField(null=True)
    private_university_or_coll = models.BooleanField(null=True)
    product_or_service_code = models.TextField(null=True)
    product_or_service_description = models.TextField(null=True)
    program_acronym = models.TextField(null=True)
    program_system_or_equ_desc = models.TextField(null=True)
    program_system_or_equipmen = models.TextField(null=True)
    pulled_from = models.TextField(null=True)
    purchase_card_as_paym_desc = models.TextField(null=True)
    purchase_card_as_payment_m = models.TextField(null=True)
    receives_contracts_and_gra = models.BooleanField(null=True)
    recovered_materials_s_desc = models.TextField(null=True)
    recovered_materials_sustai = models.TextField(null=True)
    referenced_idv_agency_desc = models.TextField(null=True)
    referenced_idv_agency_iden = models.TextField(null=True)
    referenced_idv_modificatio = models.TextField(null=True)
    referenced_idv_type = models.TextField(null=True)
    referenced_idv_type_desc = models.TextField(null=True)
    referenced_mult_or_si_desc = models.TextField(null=True)
    referenced_mult_or_single = models.TextField(null=True)
    research = models.TextField(null=True)
    research_description = models.TextField(null=True)
    sam_exception = models.TextField(null=True)
    sam_exception_description = models.TextField(null=True)
    sba_certified_8_a_joint_ve = models.BooleanField(null=True)
    school_district_local_gove = models.BooleanField(null=True)
    school_of_forestry = models.BooleanField(null=True)
    sea_transportation = models.TextField(null=True)
    sea_transportation_desc = models.TextField(null=True)
    self_certified_small_disad = models.BooleanField(null=True)
    service_disabled_veteran_o = models.BooleanField(null=True)
    small_agricultural_coopera = models.BooleanField(null=True)
    small_business_competitive = models.BooleanField(null=True)
    small_disadvantaged_busine = models.BooleanField(null=True)
    sole_proprietorship = models.BooleanField(null=True)
    solicitation_date = models.DateField(null=True)
    solicitation_identifier = models.TextField(null=True)
    solicitation_procedur_desc = models.TextField(null=True)
    solicitation_procedures = models.TextField(null=True)
    state_controlled_instituti = models.BooleanField(null=True)
    subchapter_s_corporation = models.BooleanField(null=True)
    subcontinent_asian_asian_i = models.BooleanField(null=True)
    subcontracting_plan = models.TextField(null=True)
    subcontracting_plan_desc = models.TextField(null=True)
    the_ability_one_program = models.BooleanField(null=True)
    total_obligated_amount = models.TextField(null=True)
    township_local_government = models.BooleanField(null=True)
    transaction_number = models.TextField(null=True)
    transit_authority = models.BooleanField(null=True)
    tribal_college = models.BooleanField(null=True)
    tribally_owned_business = models.BooleanField(null=True)
    type_of_contract_pricing = models.TextField(null=True)
    type_of_contract_pric_desc = models.TextField(null=True)
    type_of_idc = models.TextField(null=True)
    type_of_idc_description = models.TextField(null=True)
    type_set_aside = models.TextField(null=True)
    type_set_aside_description = models.TextField(null=True)
    undefinitized_action = models.TextField(null=True)
    undefinitized_action_desc = models.TextField(null=True)
    us_federal_government = models.BooleanField(null=True)
    us_government_entity = models.BooleanField(null=True)
    us_local_government = models.BooleanField(null=True)
    us_state_government = models.BooleanField(null=True)
    us_tribal_government = models.BooleanField(null=True)
    vendor_doing_as_business_n = models.TextField(null=True)
    vendor_fax_number = models.TextField(null=True)
    vendor_phone_number = models.TextField(null=True)
    veteran_owned_business = models.BooleanField(null=True)
    veterinary_college = models.BooleanField(null=True)
    veterinary_hospital = models.BooleanField(null=True)
    woman_owned_business = models.BooleanField(null=True)
    women_owned_small_business = models.BooleanField(null=True)
    program_activities = models.JSONField(null=True)

    class Meta:
        db_table = "transaction_search"
        constraints = [models.UniqueConstraint(fields=["is_fpds", "transaction"], name="ts_idx_is_fpds_transaction_id")]
        indexes = [
            models.Index(fields=["transaction"], name="ts_idx_transaction_id"),
            models.Index(fields=["generated_unique_award_id"], name="ts_idx_award_key"),
            models.Index(
                fields=["afa_generated_unique"],
                name="ts_idx_fabs_key_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(
                fields=["detached_award_proc_unique"],
                name="ts_idx_fpds_key_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(fields=["piid"], name="ts_idx_piid_pre2008", condition=Q(action_date__lt="2007-10-01")),
            models.Index(
                fields=["parent_award_id"],
                name="ts_idx_parent_award_id_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(fields=["fain"], name="ts_idx_fain_pre2008", condition=Q(action_date__lt="2007-10-01")),
            models.Index(fields=["uri"], name="ts_idx_uri_pre2008", condition=Q(action_date__lt="2007-10-01")),
            models.Index(fields=["is_fpds"], name="ts_idx_is_fpds"),
            models.Index(
                fields=["-action_date"], name="ts_idx_action_date", condition=Q(action_date__gte="2007-10-01")
            ),
            models.Index(fields=["-last_modified_date"], name="ts_idx_last_modified_date"),
            models.Index(
                fields=["-fiscal_year"], name="ts_idx_fiscal_year", condition=Q(action_date__gte="2007-10-01")
            ),
            models.Index(
                fields=["type"], name="ts_idx_type", condition=Q(type__isnull=False) & Q(action_date__gte="2007-10-01")
            ),
            models.Index(fields=["award"], name="ts_idx_award_id", condition=Q(action_date__gte="2007-10-01")),
            models.Index(
                fields=["pop_zip5"],
                name="ts_idx_pop_zip5",
                condition=Q(pop_zip5__isnull=False) & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_unique_id"],
                name="ts_idx_recipient_unique_id",
                condition=Q(recipient_unique_id__isnull=False) & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["parent_recipient_unique_id"],
                name="ts_idx_parent_recipient_unique",
                condition=Q(parent_recipient_unique_id__isnull=False) & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["pop_state_code", "action_date"],
                name="ts_idx_simple_pop_geolocation",
                condition=Q(pop_country_code="USA")
                & Q(pop_state_code__isnull=False)
                & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_hash"], name="ts_idx_recipient_hash", condition=Q(action_date__gte="2007-10-01")
            ),
            models.Index(
                fields=["action_date"], name="ts_idx_action_date_pre2008", condition=Q(action_date__lt="2007-10-01")
            ),
            models.Index(fields=["etl_update_date"], name="ts_idx_etl_update_date"),
            models.Index(
                fields=["type_of_contract_pricing"],
                name="ts_idx_tocp_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(fields=["naics_code"], name="ts_idx_naics_pre2008", condition=Q(action_date__lt="2007-10-01")),
            models.Index(
                fields=["extent_competed"], name="ts_idx_ext_com_pre2008", condition=Q(action_date__lt="2007-10-01")
            ),
            models.Index(
                fields=["product_or_service_code"], name="ts_idx_psc_pre2008", condition=Q(action_date__lt="2007-10-01")
            ),
            models.Index(
                fields=["type_set_aside"],
                name="ts_idx_type_set_aside_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(
                fields=["cfda_number"], name="ts_idx_cfda_aside_pre2008", condition=Q(action_date__lt="2007-10-01")
            ),
            models.Index(fields=["awarding_agency_id"], name="ts_idx_awarding_agency_id"),
            models.Index(fields=["funding_agency_id"], name="ts_idx_funding_agency_id"),
        ]
