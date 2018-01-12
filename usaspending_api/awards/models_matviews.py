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
    generated_unique_award_id = models.TextField(blank=True, null=True)
    category = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    type_description = models.TextField(blank=True, null=True)
    agency_id = models.TextField(blank=True, null=True)
    referenced_idv_agency_iden = models.TextField(blank=True, null=True)
    referenced_idv_agency_desc = models.TextField(blank=True, null=True)
    multiple_or_single_award_i = models.TextField(blank=True, null=True)
    multiple_or_single_aw_desc = models.TextField(blank=True, null=True)
    piid = models.TextField(blank=True, null=True)
    parent_award_piid = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    # agencies
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_agency_abbr = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_abbr = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)

    funding_agency_id = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_agency_abbr = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_abbr = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)

    data_source = models.TextField(blank=True, null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    date_signed = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    period_of_performance_start_date = models.DateTimeField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateTimeField(blank=True, null=True)
    potential_total_value_of_award = models.DecimalField(max_digits=23, decimal_places=2,
                                                         blank=True, null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True,
                                                     null=True)
    last_modified_date = models.DateTimeField(blank=True, null=True)
    certified_date = models.DateTimeField(blank=True, null=True)
    record_type = models.IntegerField(blank=True, null=True)
    latest_transaction_unique_id = models.TextField(blank=True, null=True)
    total_subaward_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True,
                                                null=True)
    subaward_count = models.IntegerField(blank=True, null=True)
    pulled_from = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField(blank=True, null=True)
    product_or_service_co_desc = models.TextField(blank=True, null=True)
    extent_competed = models.TextField(blank=True, null=True)
    extent_compete_description = models.TextField(blank=True, null=True)
    type_of_contract_pricing = models.TextField(blank=True, null=True)
    type_of_contract_pric_desc = models.TextField(blank=True, null=True)
    contract_award_type_desc = models.TextField(blank=True, null=True)
    cost_or_pricing_data = models.TextField(blank=True, null=True)
    cost_or_pricing_data_desc = models.TextField(blank=True, null=True)
    domestic_or_foreign_entity = models.TextField(blank=True, null=True)
    domestic_or_foreign_e_desc = models.TextField(blank=True, null=True)
    fair_opportunity_limited_s = models.TextField(blank=True, null=True)
    fair_opportunity_limi_desc = models.TextField(blank=True, null=True)
    foreign_funding = models.TextField(blank=True, null=True)
    foreign_funding_desc = models.TextField(blank=True, null=True)
    interagency_contracting_au = models.TextField(blank=True, null=True)
    interagency_contract_desc = models.TextField(blank=True, null=True)
    major_program = models.TextField(blank=True, null=True)
    price_evaluation_adjustmen = models.TextField(blank=True, null=True)
    program_acronym = models.TextField(blank=True, null=True)
    subcontracting_plan = models.TextField(blank=True, null=True)
    subcontracting_plan_desc = models.TextField(blank=True, null=True)
    multi_year_contract = models.TextField(blank=True, null=True)
    multi_year_contract_desc = models.TextField(blank=True, null=True)
    purchase_card_as_payment_m = models.TextField(blank=True, null=True)
    purchase_card_as_paym_desc = models.TextField(blank=True, null=True)
    consolidated_contract = models.TextField(blank=True, null=True)
    consolidated_contract_desc = models.TextField(blank=True, null=True)
    solicitation_identifier = models.TextField(blank=True, null=True)
    solicitation_procedures = models.TextField(blank=True, null=True)
    solicitation_procedur_desc = models.TextField(blank=True, null=True)
    number_of_offers_received = models.TextField(blank=True, null=True)
    other_than_full_and_open_c = models.TextField(blank=True, null=True)
    other_than_full_and_o_desc = models.TextField(blank=True, null=True)
    commercial_item_acquisitio = models.TextField(blank=True, null=True)
    commercial_item_acqui_desc = models.TextField(blank=True, null=True)
    commercial_item_test_progr = models.TextField(blank=True, null=True)
    commercial_item_test_desc = models.TextField(blank=True, null=True)
    evaluated_preference = models.TextField(blank=True, null=True)
    evaluated_preference_desc = models.TextField(blank=True, null=True)
    fed_biz_opps = models.TextField(blank=True, null=True)
    fed_biz_opps_description = models.TextField(blank=True, null=True)
    small_business_competitive = models.TextField(blank=True, null=True)
    dod_claimant_program_code = models.TextField(blank=True, null=True)
    dod_claimant_prog_cod_desc = models.TextField(blank=True, null=True)
    program_system_or_equipmen = models.TextField(blank=True, null=True)
    program_system_or_equ_desc = models.TextField(blank=True, null=True)
    information_technology_com = models.TextField(blank=True, null=True)
    information_technolog_desc = models.TextField(blank=True, null=True)
    sea_transportation = models.TextField(blank=True, null=True)
    sea_transportation_desc = models.TextField(blank=True, null=True)
    clinger_cohen_act_planning = models.TextField(blank=True, null=True)
    clinger_cohen_act_pla_desc = models.TextField(blank=True, null=True)
    davis_bacon_act = models.TextField(blank=True, null=True)
    davis_bacon_act_descrip = models.TextField(blank=True, null=True)
    service_contract_act = models.TextField(blank=True, null=True)
    service_contract_act_desc = models.TextField(blank=True, null=True)
    walsh_healey_act = models.TextField(blank=True, null=True)
    walsh_healey_act_descrip = models.TextField(blank=True, null=True)
    naics = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True)
    idv_type = models.TextField(blank=True, null=True)
    idv_type_description = models.TextField(blank=True, null=True)
    type_set_aside = models.TextField(blank=True, null=True)
    type_set_aside_description = models.TextField(blank=True, null=True)
    assistance_type = models.TextField(blank=True, null=True)
    original_loan_subsidy_cost = models.TextField(blank=True, null=True)
    business_funds_indicator = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    business_types_description = models.TextField(blank=True, null=True)
    business_categories = ArrayField(models.TextField(), default=list)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    cfda_objectives = models.TextField(blank=True, null=True)

    # recipient data
    recipient_unique_id = models.TextField(blank=True, null=True)  # DUNS
    recipient_name = models.TextField(blank=True, null=True)
    parent_recipient_unique_id = models.TextField(blank=True, null=True)

    # executive compensation data
    officer_1_name = models.TextField(blank=True, null=True)
    officer_1_amount = models.TextField(blank=True, null=True)
    officer_2_name = models.TextField(blank=True, null=True)
    officer_2_amount = models.TextField(blank=True, null=True)
    officer_3_name = models.TextField(blank=True, null=True)
    officer_3_amount = models.TextField(blank=True, null=True)
    officer_4_name = models.TextField(blank=True, null=True)
    officer_4_amount = models.TextField(blank=True, null=True)
    officer_5_name = models.TextField(blank=True, null=True)
    officer_5_amount = models.TextField(blank=True, null=True)

    # business categories
    recipient_location_address_line1 = models.TextField(blank=True, null=True)
    recipient_location_address_line2 = models.TextField(blank=True, null=True)
    recipient_location_address_line3 = models.TextField(blank=True, null=True)

    # foreign province
    recipient_location_foreign_province = models.TextField(blank=True, null=True)
    recipient_location_foreign_city_name = models.TextField(blank=True, null=True)
    recipient_location_foreign_postal_code = models.TextField(blank=True, null=True)

    # country
    recipient_location_country_code = models.TextField(blank=True, null=True)
    recipient_location_country_name = models.TextField(blank=True, null=True)

    # state
    recipient_location_state_code = models.TextField(blank=True, null=True)
    recipient_location_state_name = models.TextField(blank=True, null=True)

    # county (NONE FOR FPDS)
    recipient_location_county_code = models.TextField(blank=True, null=True)
    recipient_location_county_name = models.TextField(blank=True, null=True)

    # city
    recipient_location_city_code = models.TextField(blank=True, null=True)
    recipient_location_city_name = models.TextField(blank=True, null=True)

    # zip
    recipient_location_zip5 = models.TextField(blank=True, null=True)
    recipient_location_zip4 = models.TextField(blank=True, null=True)

    # congressional district
    recipient_location_congressional_code = models.TextField(blank=True, null=True)

    # ppop data
    pop_code = models.TextField(blank=True, null=True)

    # foreign
    pop_foreign_province = models.TextField(blank=True, null=True)

    # country
    pop_country_code = models.TextField(blank=True, null=True)
    pop_country_name = models.TextField(blank=True, null=True)

    # state
    pop_state_code = models.TextField(blank=True, null=True)
    pop_state_name = models.TextField(blank=True, null=True)

    # county
    pop_county_code = models.TextField(blank=True, null=True)
    pop_county_name = models.TextField(blank=True, null=True)

    # city
    pop_city_name = models.TextField(blank=True, null=True)

    # zip
    pop_zip5 = models.TextField(blank=True, null=True)
    pop_zip4 = models.TextField(blank=True, null=True)

    # congressional district
    pop_congressional_code = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'award_matview'


class AwardCategory(models.Model):
    type_code = models.TextField(blank=True, null=False)
    type_name = models.TextField(blank=True, null=False)

    class Meta:
        managed = False
        db_table = 'award_category'


class TransactionMatView(models.Model):
    # unique ids + cols used for unique id
    detached_award_proc_unique = models.TextField(blank=True, null=True)
    afa_generated_unique = models.TextField(blank=True, null=True)
    piid = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    agency_id = models.TextField(blank=True, null=True)
    referenced_idv_agency_iden = models.TextField(blank=True, null=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    transaction_number = models.IntegerField(blank=True, null=True)

    # duns
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)

    # recipient
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_foreign_provi = models.TextField(blank=True, null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_name = models.TextField(blank=True, null=True)
    legal_entity_county_code = models.TextField(blank=True, null=True)
    legal_entity_county_name = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_zip5 = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)

    # place of performance
    place_of_perform_country_code = models.TextField(blank=True, null=True)
    place_of_perform_country_name = models.TextField(blank=True, null=True)
    place_of_perform_state_code = models.TextField(blank=True, null=True)
    place_of_perform_state_name = models.TextField(blank=True, null=True)
    place_of_perform_county_code = models.TextField(blank=True, null=True)
    place_of_perform_county_name = models.TextField(blank=True, null=True)
    place_of_perform_city_name = models.TextField(blank=True, null=True)
    place_of_performance_zip5 = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)

    # other(fpds specific)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    contract_award_type = models.TextField(blank=True, null=True)
    contract_award_type_desc = models.TextField(blank=True, null=True)
    referenced_idv_type = models.TextField(blank=True, null=True)
    referenced_idv_type_desc = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True,
                                                    null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    period_of_performance_star = models.DateTimeField(blank=True, null=True)
    period_of_performance_curr = models.DateTimeField(blank=True, null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True,
                                                     null=True)
    last_modified_date = models.DateTimeField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    pulled_from = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField(blank=True, null=True)
    product_or_service_co_desc = models.TextField(blank=True, null=True)
    extent_competed = models.TextField(blank=True, null=True)
    extent_compete_description = models.TextField(blank=True, null=True)
    type_of_contract_pricing = models.TextField(blank=True, null=True)
    naics = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)
    idv_type = models.TextField(blank=True, null=True)
    idv_type_description = models.TextField(blank=True, null=True)
    type_set_aside = models.TextField(blank=True, null=True)
    type_set_aside_description = models.TextField(blank=True, null=True)

    # other(fabs specific)
    assistance_type = models.TextField(blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True,
                                                     null=True)
    record_type = models.IntegerField(blank=True, null=True)
    business_funds_indicator = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'transaction_matview'


class TransactionFPDSMatview(models.Model):
    generated_unique_award_id = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    type_description = models.TextField(blank=True, null=True)
    category = models.TextField(blank=True, null=False)
    agency_id = models.TextField(blank=True, null=True)
    referenced_idv_agency_iden = models.TextField(blank=True, null=True)
    piid = models.TextField(blank=True, null=True)
    parent_award_piid = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    funding_agency_id = models.TextField(blank=True, null=True)
    data_source = models.TextField(blank=True, null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    fiscal_year = models.IntegerField(blank=True, null=True, help_text="Fiscal Year calculated based on Action Date")
    date_signed = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    period_of_performance_start_date = models.DateTimeField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateTimeField(blank=True, null=True)
    potential_total_value_of_award = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateTimeField(blank=True, null=True)
    certified_date = models.DateTimeField(blank=True, null=True)
    latest_transaction_id = models.IntegerField(blank=True, null=True)
    latest_transaction_unique = models.TextField(blank=True, null=True)
    total_subaward_amount = models.IntegerField(blank=True, null=True)
    subaward_count = models.IntegerField(blank=True, null=True)

    # recipient data
    recipient_unique_id = models.TextField(blank=True, null=True) # DUNS
    recipient_name = models.TextField(blank=True, null=True)

    # executive compensation data
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    # business categories
    recipient_location_address_line1 = models.TextField(blank=True, null=True)
    recipient_location_address_line2 = models.TextField(blank=True, null=True)
    recipient_location_address_line3 = models.TextField(blank=True, null=True)

    # foreign province
    recipient_location_foreign_province = models.TextField(blank=True, null=True)

    # country
    recipient_location_country_code = models.TextField(blank=True, null=True)
    recipient_location_country_name = models.TextField(blank=True, null=True)

    # state
    recipient_location_state_code = models.TextField(blank=True, null=True)
    recipient_location_state_name = models.TextField(blank=True, null=True)

    # county (NONE FOR FPDS)
    recipient_location_county_code = models.TextField(blank=True, null=True)
    recipient_location_county_name = models.TextField(blank=True, null=True)

    # city
    recipient_location_city_name = models.TextField(blank=True, null=True)

    # zip
    recipient_location_zip5 = models.TextField(blank=True, null=True)

    # congressional district
    recipient_location_congressional_code = models.TextField(blank=True, null=True)

    # ppop data

    # foreign
    pop_foreign_province = models.TextField(blank=True, null=True)

    # country
    pop_country_code = models.TextField(blank=True, null=True)
    pop_country_name = models.TextField(blank=True, null=True)

    # state
    pop_state_code = models.TextField(blank=True, null=True)
    pop_state_name = models.TextField(blank=True, null=True)

    # county
    pop_county_code = models.TextField(blank=True, null=True)
    pop_county_name = models.TextField(blank=True, null=True)

    # city
    pop_city_name = models.TextField(blank=True, null=True)

    # zip
    pop_zip5 = models.TextField(blank=True, null=True)
    pop_zip4 = models.TextField(blank=True, null=True)

    # congressional district
    pop_congressional_code = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'transaction_fpds_matview'


class TransactionFABSFAINMatview(models.Model):
    generated_unique_award_id = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    type_description = models.TextField(blank=True, null=True)
    category = models.TextField(blank=True, null=False)
    piid = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    funding_agency_id = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    data_source = models.TextField(blank=True, null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    fiscal_year = models.IntegerField(blank=True, null=True, help_text="Fiscal Year calculated based on Action Date")
    date_signed = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    period_of_performance_start_date = models.DateTimeField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateTimeField(blank=True, null=True)
    potential_total_value_of_award = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateTimeField(blank=True, null=True)
    certified_date = models.DateTimeField(blank=True, null=True)
    latest_transaction_id = models.IntegerField(blank=True, null=True)
    record_type = models.TextField(blank=True, null=True)
    latest_transaction_unique = models.TextField(blank=True, null=True)
    total_subaward_amount = models.IntegerField(blank=True, null=True)
    subaward_count = models.IntegerField(blank=True, null=True)

    # recipient data
    recipient_unique_id = models.TextField(blank=True, null=True) # DUNS
    recipient_name = models.TextField(blank=True, null=True)

    # executive compensation data
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    # business categories
    recipient_location_address_line1 = models.TextField(blank=True, null=True)
    recipient_location_address_line2 = models.TextField(blank=True, null=True)
    recipient_location_address_line3 = models.TextField(blank=True, null=True)

    # foreign province
    recipient_location_foreign_province = models.TextField(blank=True, null=True)

    # country
    recipient_location_country_code = models.TextField(blank=True, null=True)
    recipient_location_country_name = models.TextField(blank=True, null=True)

    # state
    recipient_location_state_code = models.TextField(blank=True, null=True)
    recipient_location_state_name = models.TextField(blank=True, null=True)

    # county
    recipient_location_county_code = models.TextField(blank=True, null=True)
    recipient_location_county_name = models.TextField(blank=True, null=True)

    # city
    recipient_location_city_name = models.TextField(blank=True, null=True)

    # zip
    recipient_location_zip5 = models.TextField(blank=True, null=True)

    # congressional district
    recipient_location_congressional_code = models.TextField(blank=True, null=True)

    # ppop data

    # foreign
    pop_foreign_province = models.TextField(blank=True, null=True)

    # country
    pop_country_code = models.TextField(blank=True, null=True)
    pop_country_name = models.TextField(blank=True, null=True)

    # state
    pop_state_code = models.TextField(blank=True, null=True)
    pop_state_name = models.TextField(blank=True, null=True)

    # county
    pop_county_code = models.TextField(blank=True, null=True)
    pop_county_name = models.TextField(blank=True, null=True)

    # city
    pop_city_name = models.TextField(blank=True, null=True)

    # zip
    pop_zip5 = models.TextField(blank=True, null=True)
    pop_zip4 = models.TextField(blank=True, null=True)

    # congressional district
    pop_congressional_code = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'transaction_fabs_fain_matview'

class TransactionFABSURIMatview(models.Model):
    generated_unique_award_id = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    type_description = models.TextField(blank=True, null=True)
    category = models.TextField(blank=True, null=False)
    piid = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    funding_agency_id = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    data_source = models.TextField(blank=True, null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    fiscal_year = models.IntegerField(blank=True, null=True, help_text="Fiscal Year calculated based on Action Date")
    date_signed = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    period_of_performance_start_date = models.DateTimeField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateTimeField(blank=True, null=True)
    potential_total_value_of_award = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateTimeField(blank=True, null=True)
    certified_date = models.DateTimeField(blank=True, null=True)
    latest_transaction_id = models.IntegerField(blank=True, null=True)
    record_type = models.TextField(blank=True, null=True)
    latest_transaction_unique = models.TextField(blank=True, null=True)
    total_subaward_amount = models.IntegerField(blank=True, null=True)
    subaward_count = models.IntegerField(blank=True, null=True)

    # recipient data
    recipient_unique_id = models.TextField(blank=True, null=True) # DUNS
    recipient_name = models.TextField(blank=True, null=True)

    # executive compensation data
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    # business categories
    recipient_location_address_line1 = models.TextField(blank=True, null=True)
    recipient_location_address_line2 = models.TextField(blank=True, null=True)
    recipient_location_address_line3 = models.TextField(blank=True, null=True)

    # foreign province
    recipient_location_foreign_province = models.TextField(blank=True, null=True)

    # country
    recipient_location_country_code = models.TextField(blank=True, null=True)
    recipient_location_country_name = models.TextField(blank=True, null=True)

    # state
    recipient_location_state_code = models.TextField(blank=True, null=True)
    recipient_location_state_name = models.TextField(blank=True, null=True)

    # county
    recipient_location_county_code = models.TextField(blank=True, null=True)
    recipient_location_county_name = models.TextField(blank=True, null=True)

    # city
    recipient_location_city_name = models.TextField(blank=True, null=True)

    # zip
    recipient_location_zip5 = models.TextField(blank=True, null=True)

    # congressional district
    recipient_location_congressional_code = models.TextField(blank=True, null=True)

    # ppop data

    # foreign
    pop_foreign_province = models.TextField(blank=True, null=True)

    # country
    pop_country_code = models.TextField(blank=True, null=True)
    pop_country_name = models.TextField(blank=True, null=True)

    # state
    pop_state_code = models.TextField(blank=True, null=True)
    pop_state_name = models.TextField(blank=True, null=True)

    # county
    pop_county_code = models.TextField(blank=True, null=True)
    pop_county_name = models.TextField(blank=True, null=True)

    # city
    pop_city_name = models.TextField(blank=True, null=True)

    # zip
    pop_zip5 = models.TextField(blank=True, null=True)
    pop_zip4 = models.TextField(blank=True, null=True)

    # congressional district
    pop_congressional_code = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'transaction_fabs_uri_matview'
