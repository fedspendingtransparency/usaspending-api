import warnings
from django.db import models
from django.core.cache import CacheKeyWarning
from django.contrib.postgres.fields import ArrayField

warnings.simplefilter("ignore", CacheKeyWarning)


class UniversalTransactionView(models.Model):
    transaction_id = models.IntegerField(blank=False, null=False, primary_key=True)
    action_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    action_type = models.TextField()
    award_id = models.IntegerField()
    award_category = models.TextField()
    total_obligation = models.DecimalField(
        max_digits=15, decimal_places=2, blank=True,
        null=True)
    total_obl_bin = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    piid = models.TextField()
    federal_action_obligation = models.DecimalField(
        max_digits=20, db_index=True, decimal_places=2, blank=True,
        null=True)

    pop_location_id = models.IntegerField()
    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_state_code = models.TextField()
    pop_congressional_code = models.TextField()

    face_value_loan_guarantee = models.TextField()
    original_loan_subsidy_cost = models.TextField()
    transaction_description = models.TextField()
    awarding_agency_id = models.IntegerField()
    awarding_agency_code = models.TextField()
    awarding_agency_name = models.TextField()
    funding_agency_id = models.IntegerField()
    funding_agency_code = models.TextField()
    funding_agency_name = models.TextField()

    naics_code = models.TextField()
    naics_description = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()

    pulled_from = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()

    cfda_number = models.TextField()
    cfda_title = models.TextField()
    cfda_popular_name = models.TextField()

    recipient_id = models.IntegerField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_categories = ArrayField(models.TextField(), default=list)

    recipient_location_id = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_congressional_code = models.TextField()

    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

    class Meta:
        managed = False
        db_table = 'universal_transaction_matview'


class SummaryTransactionView(models.Model):
    action_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    total_obl_bin = models.TextField()
    federal_action_obligation = models.DecimalField(
        max_digits=20, db_index=True, decimal_places=2, blank=True,
        null=True)

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_zip5 = models.TextField()
    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_state_code = models.TextField()
    pop_congressional_code = models.TextField()

    awarding_toptier_agency_name = models.TextField(blank=True, null=True)
    awarding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_toptier_agency_name = models.TextField(blank=True, null=True)
    funding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    awarding_subtier_agency_name = models.TextField(blank=True, null=True)
    awarding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_subtier_agency_name = models.TextField(blank=True, null=True)
    funding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)

    business_categories = ArrayField(models.TextField(), default=list)
    cfda_number = models.TextField()
    cfda_title = models.TextField()
    cfda_popular_name = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField()
    naics_description = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_view'


class UniversalAwardView(models.Model):
    award_id = models.IntegerField(blank=False, null=False, primary_key=True)
    category = models.TextField()
    type = models.TextField()
    type_description = models.TextField()
    piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    total_obligation = models.DecimalField(
        max_digits=15, decimal_places=2, blank=True,
        null=True)
    total_obl_bin = models.TextField()

    recipient_id = models.IntegerField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_categories = ArrayField(models.TextField(), default=list)

    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    period_of_performance_start_date = models.DateField()
    period_of_performance_current_end_date = models.DateField()

    face_value_loan_guarantee = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True,
        null=True)
    original_loan_subsidy_cost = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True,
        null=True)

    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()

    recipient_location_country_name = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()

    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()

    cfda_number = models.TextField()
    pulled_from = models.TextField()
    type_of_contract_pricing = models.TextField()
    extent_competed = models.TextField()
    type_set_aside = models.TextField()

    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField()
    naics_description = models.TextField()

    class Meta:
        managed = False
        db_table = 'universal_award_matview'


class SummaryAwardView(models.Model):
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    category = models.TextField(blank=True, null=True)
    awarding_toptier_agency_name = models.TextField(blank=True, null=True)
    awarding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_toptier_agency_name = models.TextField(blank=True, null=True)
    funding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    awarding_subtier_agency_name = models.TextField(blank=True, null=True)
    awarding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_subtier_agency_name = models.TextField(blank=True, null=True)
    funding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_award_view'


class SummaryView(models.Model):
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    awarding_toptier_agency_name = models.TextField(blank=True, null=True)
    awarding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_toptier_agency_name = models.TextField(blank=True, null=True)
    funding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    awarding_subtier_agency_name = models.TextField(blank=True, null=True)
    awarding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_subtier_agency_name = models.TextField(blank=True, null=True)
    funding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view'


class SummaryNaicsCodesView(models.Model):
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    naics_code = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_naics_codes'


class SummaryPscCodesView(models.Model):
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    product_or_service_code = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_psc_codes'


class SummaryCfdaNumbersView(models.Model):
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_cfda_number'


class SummaryTransactionMonthView(models.Model):
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()
    pulled_from = models.TextField()

    recipient_location_country_name = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_foreign_province = models.TextField()

    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_state_code = models.TextField()
    pop_county_name = models.TextField()
    pop_county_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()

    awarding_toptier_agency_name = models.TextField(blank=True, null=True)
    awarding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_toptier_agency_name = models.TextField(blank=True, null=True)
    funding_toptier_agency_abbreviation = models.TextField(blank=True, null=True)
    awarding_subtier_agency_name = models.TextField(blank=True, null=True)
    awarding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)
    funding_subtier_agency_name = models.TextField(blank=True, null=True)
    funding_subtier_agency_abbreviation = models.TextField(blank=True, null=True)

    business_categories = ArrayField(models.TextField(), default=list)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    cfda_popular_name = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)

    total_obl_bin = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_month_view'


class AwardMatview(models.Model):
    generated_unique_award_id = models.TextField()
    category = models.TextField()
    type = models.TextField()
    type_description = models.TextField()
    agency_id = models.TextField()
    referenced_idv_agency_iden = models.TextField()
    referenced_idv_agency_desc = models.TextField()
    multiple_or_single_award_i = models.TextField()
    multiple_or_single_aw_desc = models.TextField()
    piid = models.TextField()
    parent_award_piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    total_obligation = models.DecimalField()
    total_outlay = models.DecimalField()

    # agencies
    awarding_agency_id = models.TextField()
    awarding_agency_code = models.TextField()
    awarding_agency_name = models.TextField()
    awarding_agency_abbr = models.TextField()
    awarding_sub_tier_agency_c = models.TextField()
    awarding_sub_tier_agency_n = models.TextField()
    awarding_sub_tier_agency_abbr = models.TextField()
    awarding_office_code = models.TextField()
    awarding_office_name = models.TextField()

    funding_agency_id = models.TextField()
    funding_agency_code = models.TextField()
    funding_agency_name = models.TextField()
    funding_agency_abbr = models.TextField()
    funding_sub_tier_agency_co = models.TextField()
    funding_sub_tier_agency_na = models.TextField()
    funding_sub_tier_agency_abbr = models.TextField()
    funding_office_code = models.TextField()
    funding_office_name = models.TextField()

    data_source = models.TextField()
    action_date = models.DateTimeField()
    date_signed = models.DateTimeField()
    description = models.TextField()
    period_of_performance_start_date = models.DateTimeField()
    period_of_performance_current_end_date = models.DateTimeField()
    potential_total_value_of_award = models.DecimalField()
    base_and_all_options_value = models.DecimalField()
    last_modified_date = models.DateTimeField()
    certified_date = models.DateTimeField()
    record_type = models.IntegerField()
    latest_transaction_unique_id = models.TextField()
    total_subaward_amount = models.DecimalField()
    subaward_count = models.IntegerField()
    pulled_from = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_co_desc = models.TextField()
    extent_competed = models.TextField()
    extent_compete_description = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_of_contract_pric_desc = models.TextField()
    contract_award_type_desc = models.TextField()
    cost_or_pricing_data = models.TextField()
    cost_or_pricing_data_desc = models.TextField()
    domestic_or_foreign_entity = models.TextField()
    domestic_or_foreign_e_desc = models.TextField()
    fair_opportunity_limited_s = models.TextField()
    fair_opportunity_limi_desc = models.TextField()
    foreign_funding = models.TextField()
    foreign_funding_desc = models.TextField()
    interagency_contracting_au = models.TextField()
    interagency_contract_desc = models.TextField()
    major_program = models.TextField()
    price_evaluation_adjustmen = models.TextField()
    program_acronym = models.TextField()
    subcontracting_plan = models.TextField()
    subcontracting_plan_desc = models.TextField()
    multi_year_contract = models.TextField()
    multi_year_contract_desc = models.TextField()
    purchase_card_as_payment_m = models.TextField()
    purchase_card_as_paym_desc = models.TextField()
    consolidated_contract = models.TextField()
    consolidated_contract_desc = models.TextField()
    solicitation_identifier = models.TextField()
    solicitation_procedures = models.TextField()
    solicitation_procedur_desc = models.TextField()
    number_of_offers_received = models.TextField()
    other_than_full_and_open_c = models.TextField()
    other_than_full_and_o_desc = models.TextField()
    commercial_item_acquisitio = models.TextField()
    commercial_item_acqui_desc = models.TextField()
    commercial_item_test_progr = models.TextField()
    commercial_item_test_desc = models.TextField()
    evaluated_preference = models.TextField()
    evaluated_preference_desc = models.TextField()
    fed_biz_opps = models.TextField()
    fed_biz_opps_description = models.TextField()
    small_business_competitive = models.TextField()
    dod_claimant_program_code = models.TextField()
    dod_claimant_prog_cod_desc = models.TextField()
    program_system_or_equipmen = models.TextField()
    program_system_or_equ_desc = models.TextField()
    information_technology_com = models.TextField()
    information_technolog_desc = models.TextField()
    sea_transportation = models.TextField()
    sea_transportation_desc = models.TextField()
    clinger_cohen_act_planning = models.TextField()
    clinger_cohen_act_pla_desc = models.TextField()
    davis_bacon_act = models.TextField()
    davis_bacon_act_descrip = models.TextField()
    service_contract_act = models.TextField()
    service_contract_act_desc = models.TextField()
    walsh_healey_act = models.TextField()
    walsh_healey_act_descrip = models.TextField()
    naics = models.TextField()
    naics_description = models.TextField()
    parent_award_id = models.TextField()
    idv_type = models.TextField()
    idv_type_description = models.TextField()
    type_set_aside = models.TextField()
    type_set_aside_description = models.TextField()
    assistance_type = models.TextField()
    original_loan_subsidy_cost = models.TextField()
    business_funds_indicator = models.TextField()
    business_types = models.TextField()
    business_types_description = models.TextField()
    business_categories = ArrayField(models.TextField())
    cfda_number = models.TextField()
    cfda_title = models.TextField()
    cfda_objectives = models.TextField()

    # recipient data
    recipient_unique_id = models.TextField()  # DUNS
    recipient_name = models.TextField()
    parent_recipient_unique_id = models.TextField()

    # executive compensation data
    officer_1_name = models.TextField()
    officer_1_amount = models.TextField()
    officer_2_name = models.TextField()
    officer_2_amount = models.TextField()
    officer_3_name = models.TextField()
    officer_3_amount = models.TextField()
    officer_4_name = models.TextField()
    officer_4_amount = models.TextField()
    officer_5_name = models.TextField()
    officer_5_amount = models.TextField()

    # business categories
    recipient_location_address_line1 = models.TextField()
    recipient_location_address_line2 = models.TextField()
    recipient_location_address_line3 = models.TextField()

    # foreign province
    recipient_location_foreign_province = models.TextField()
    recipient_location_foreign_city_name = models.TextField()
    recipient_location_foreign_postal_code = models.TextField()

    # country
    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()

    # state
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()

    # county (NONE FOR FPDS)
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()

    # city
    recipient_location_city_code = models.TextField()
    recipient_location_city_name = models.TextField()

    # zip
    recipient_location_zip5 = models.TextField()
    recipient_location_zip4 = models.TextField()

    # congressional district
    recipient_location_congressional_code = models.TextField()

    # ppop data
    pop_code = models.TextField()

    # foreign
    pop_foreign_province = models.TextField()

    # country
    pop_country_code = models.TextField()
    pop_country_name = models.TextField()

    # state
    pop_state_code = models.TextField()
    pop_state_name = models.TextField()

    # county
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()

    # city
    pop_city_name = models.TextField()

    # zip
    pop_zip5 = models.TextField()
    pop_zip4 = models.TextField()

    # congressional district
    pop_congressional_code = models.TextField()

    class Meta:
        managed = False
        db_table = 'award_matview'


class AwardCategory(models.Model):
    type_code = models.TextField()
    type_name = models.TextField()

    class Meta:
        managed = False
        db_table = 'award_category'


class TransactionMatView(models.Model):
    # unique ids + cols used for unique id
    detached_award_proc_unique = models.TextField()
    afa_generated_unique = models.TextField()
    piid = models.TextField()
    parent_award_id = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    agency_id = models.TextField()
    referenced_idv_agency_iden = models.TextField()
    award_modification_amendme = models.TextField()
    transaction_number = models.IntegerField()

    # duns
    awardee_or_recipient_uniqu = models.TextField()
    awardee_or_recipient_legal = models.TextField()

    # recipient
    legal_entity_address_line1 = models.TextField()
    legal_entity_address_line2 = models.TextField()
    legal_entity_address_line3 = models.TextField()
    legal_entity_foreign_provi = models.TextField()
    legal_entity_country_code = models.TextField()
    legal_entity_country_name = models.TextField()
    legal_entity_state_code = models.TextField()
    legal_entity_state_name = models.TextField()
    legal_entity_county_code = models.TextField()
    legal_entity_county_name = models.TextField()
    legal_entity_city_name = models.TextField()
    legal_entity_zip5 = models.TextField()
    legal_entity_congressional = models.TextField()

    # place of performance
    place_of_perform_country_code = models.TextField()
    place_of_perform_country_name = models.TextField()
    place_of_perform_state_code = models.TextField()
    place_of_perform_state_name = models.TextField()
    place_of_perform_county_code = models.TextField()
    place_of_perform_county_name = models.TextField()
    place_of_perform_city_name = models.TextField()
    place_of_performance_zip5 = models.TextField()
    place_of_performance_congr = models.TextField()

    # other(fpds specific)
    awarding_sub_tier_agency_c = models.TextField()
    funding_sub_tier_agency_co = models.TextField()
    contract_award_type = models.TextField()
    contract_award_type_desc = models.TextField()
    referenced_idv_type = models.TextField()
    referenced_idv_type_desc = models.TextField()
    federal_action_obligation = models.DecimalField()
    action_date = models.DateTimeField()
    award_description = models.TextField()
    period_of_performance_star = models.DateTimeField()
    period_of_performance_curr = models.DateTimeField()
    base_and_all_options_value = models.DecimalField()
    last_modified_date = models.DateTimeField()
    awarding_office_code = models.TextField()
    awarding_office_name = models.TextField()
    funding_office_code = models.TextField()
    funding_office_name = models.TextField()
    pulled_from = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_co_desc = models.TextField()
    extent_competed = models.TextField()
    extent_compete_description = models.TextField()
    type_of_contract_pricing = models.TextField()
    naics = models.TextField()
    naics_description = models.TextField()
    idv_type = models.TextField()
    idv_type_description = models.TextField()
    type_set_aside = models.TextField()
    type_set_aside_description = models.TextField()

    # other(fabs specific)
    assistance_type = models.TextField()
    original_loan_subsidy_cost = models.DecimalField()
    record_type = models.IntegerField()
    business_funds_indicator = models.TextField()
    business_types = models.TextField()
    cfda_number = models.TextField()
    cfda_title = models.TextField()

    class Meta:
        managed = False
        db_table = 'transaction_matview'
