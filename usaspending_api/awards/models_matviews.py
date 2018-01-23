import warnings
from django.db import models
from django.core.cache import CacheKeyWarning
from django.contrib.postgres.fields import ArrayField

warnings.simplefilter("ignore", CacheKeyWarning)


class UniversalTransactionView(models.Model):
    keyword_string = models.TextField()
    award_id_string = models.TextField()
    transaction_id = models.IntegerField()
    action_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    action_type = models.TextField()
    award_id = models.IntegerField()
    award_category = models.TextField()
    total_obligation = models.DecimalField(
        max_digits=15, decimal_places=2, blank=True, null=True)
    total_obl_bin = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    piid = models.TextField()
    federal_action_obligation = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True)
    transaction_description = models.TextField()
    modification_number = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()

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

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
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

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

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
    keyword_string = models.TextField()
    award_id_string = models.TextField()
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

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
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
    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()
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
    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()
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

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

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


class SummaryTransactionGeoView(models.Model):
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

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_geo_view'


class AwardMatview(models.Model):
    generated_unique_award_id = models.TextField(primary_key=True, db_column='generated_unique_award_id')
    latest_transaction = models.ForeignKey(to='awards.TransactionMatview',
                                           to_field='generated_unique_transaction_id',
                                           db_column='latest_transaction_unique_id',
                                           related_query_name='latest_transaction')

    action_date = models.TextField()
    agency_id = models.TextField()
    assistance_type = models.TextField()
    awarding_agency_abbr = models.TextField()
    awarding_agency_code = models.TextField()
    awarding_agency_id = models.TextField()
    awarding_agency_name = models.TextField()
    awarding_office_code = models.TextField()
    awarding_office_name = models.TextField()
    awarding_sub_tier_agency_abbr = models.TextField()
    awarding_sub_tier_agency_c = models.TextField()
    awarding_sub_tier_agency_n = models.TextField()
    base_and_all_options_value = models.DecimalField(max_digits=15, decimal_places=2)
    business_categories = ArrayField(models.TextField())
    business_funds_indicator = models.TextField()
    business_types = models.TextField()
    business_types_description = models.TextField()
    category = models.TextField()
    certified_date = models.DateTimeField()
    cfda_number = models.TextField()
    cfda_objectives = models.TextField()
    cfda_title = models.TextField()
    clinger_cohen_act_pla_desc = models.TextField()
    clinger_cohen_act_planning = models.TextField()
    commercial_item_acqui_desc = models.TextField()
    commercial_item_acquisitio = models.TextField()
    commercial_item_test_desc = models.TextField()
    commercial_item_test_progr = models.TextField()
    consolidated_contract = models.TextField()
    consolidated_contract_desc = models.TextField()
    contract_award_type_desc = models.TextField()
    cost_or_pricing_data = models.TextField()
    cost_or_pricing_data_desc = models.TextField()
    date_signed = models.TextField()
    davis_bacon_act = models.TextField()
    davis_bacon_act_descrip = models.TextField()
    description = models.TextField()
    dod_claimant_prog_cod_desc = models.TextField()
    dod_claimant_program_code = models.TextField()
    domestic_or_foreign_e_desc = models.TextField()
    domestic_or_foreign_entity = models.TextField()
    evaluated_preference = models.TextField()
    evaluated_preference_desc = models.TextField()
    extent_compete_description = models.TextField()
    extent_competed = models.TextField()
    fain = models.TextField()
    fair_opportunity_limi_desc = models.TextField()
    fair_opportunity_limited_s = models.TextField()
    fed_biz_opps = models.TextField()
    fed_biz_opps_description = models.TextField()
    fiscal_year = models.TextField()
    foreign_funding = models.TextField()
    foreign_funding_desc = models.TextField()
    funding_agency_abbr = models.TextField()
    funding_agency_code = models.TextField()
    funding_agency_id = models.TextField()
    funding_agency_name = models.TextField()
    funding_office_code = models.TextField()
    funding_office_name = models.TextField()
    funding_sub_tier_agency_abbr = models.TextField()
    funding_sub_tier_agency_co = models.TextField()
    funding_sub_tier_agency_na = models.TextField()
    idv_type = models.TextField()
    idv_type_description = models.TextField()
    information_technolog_desc = models.TextField()
    information_technology_com = models.TextField()
    interagency_contract_desc = models.TextField()
    interagency_contracting_au = models.TextField()
    last_modified_date = models.DateTimeField()
    major_program = models.TextField()
    multi_year_contract = models.TextField()
    multi_year_contract_desc = models.TextField()
    multiple_or_single_aw_desc = models.TextField()
    multiple_or_single_award_i = models.TextField()
    naics = models.TextField()
    naics_description = models.TextField()
    number_of_offers_received = models.TextField()
    officer_1_amount = models.TextField()
    officer_1_name = models.TextField()
    officer_2_amount = models.TextField()
    officer_2_name = models.TextField()
    officer_3_amount = models.TextField()
    officer_3_name = models.TextField()
    officer_4_amount = models.TextField()
    officer_4_name = models.TextField()
    officer_5_amount = models.TextField()
    officer_5_name = models.TextField()
    other_than_full_and_o_desc = models.TextField()
    other_than_full_and_open_c = models.TextField()
    parent_award_piid = models.TextField()
    parent_recipient_unique_id = models.TextField()
    period_of_performance_current_end_date = models.DateTimeField()
    period_of_performance_start_date = models.DateTimeField()
    piid = models.TextField()
    pop_city_name = models.TextField()
    pop_code = models.TextField()
    pop_congressional_code = models.TextField()
    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_foreign_province = models.TextField()
    pop_state_code = models.TextField()
    pop_state_name = models.TextField()
    pop_zip5 = models.TextField()
    potential_total_value_of_award = models.DecimalField(max_digits=15, decimal_places=2)
    price_evaluation_adjustmen = models.TextField()
    product_or_service_co_desc = models.TextField()
    product_or_service_code = models.TextField()
    program_acronym = models.TextField()
    program_system_or_equ_desc = models.TextField()
    program_system_or_equipmen = models.TextField()
    pulled_from = models.TextField()
    purchase_card_as_paym_desc = models.TextField()
    purchase_card_as_payment_m = models.TextField()
    recipient_location_address_line1 = models.TextField()
    recipient_location_address_line2 = models.TextField()
    recipient_location_address_line3 = models.TextField()
    recipient_location_city_code = models.TextField()
    recipient_location_city_name = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_foreign_city_name = models.TextField()
    recipient_location_foreign_postal_code = models.TextField()
    recipient_location_foreign_province = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    record_type = models.IntegerField()
    referenced_idv_agency_desc = models.TextField()
    referenced_idv_agency_iden = models.TextField()
    sai_number = models.TextField()
    sea_transportation = models.TextField()
    sea_transportation_desc = models.TextField()
    service_contract_act = models.TextField()
    service_contract_act_desc = models.TextField()
    small_business_competitive = models.TextField()
    solicitation_identifier = models.TextField()
    solicitation_procedur_desc = models.TextField()
    solicitation_procedures = models.TextField()
    subaward_count = models.IntegerField()
    subcontracting_plan = models.TextField()
    subcontracting_plan_desc = models.TextField()
    total_obligation = models.DecimalField(max_digits=15, decimal_places=2)
    total_outlay = models.DecimalField(max_digits=15, decimal_places=2)
    total_subaward_amount = models.DecimalField(max_digits=15, decimal_places=2)
    total_subsidy_cost = models.DecimalField(max_digits=15, decimal_places=2)
    type = models.TextField()
    type_description = models.TextField()
    type_of_contract_pric_desc = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_of_idc = models.TextField()
    type_of_idc_description = models.TextField()
    type_set_aside = models.TextField()
    type_set_aside_description = models.TextField()
    uri = models.TextField()
    walsh_healey_act = models.TextField()
    walsh_healey_act_descrip = models.TextField()

    class Meta:
        managed = False
        db_table = 'award_matview'


class AwardCategory(models.Model):
    type_code = models.TextField()
    type_name = models.TextField()

    class Meta:
        managed = False
        db_table = 'award_category'


class TransactionMatview(models.Model):
    generated_unique_transaction_id = models.TextField(primary_key=True, db_column='generated_unique_transaction_id')
    award = models.ForeignKey(to='awards.AwardMatview',
                              to_field='generated_unique_award_id',
                              db_column='generated_unique_award_id',
                              related_query_name='award')

    action_date = models.DateTimeField()
    agency_id = models.TextField()
    assistance_type = models.TextField()
    award_description = models.TextField()
    award_modification_amendme = models.TextField()
    awardee_or_recipient_legal = models.TextField()
    awardee_or_recipient_uniqu = models.TextField()
    awarding_agency_code = models.TextField()
    awarding_agency_name = models.TextField()
    awarding_sub_tier_agency_c = models.TextField()
    awarding_sub_tier_agency_n = models.TextField()
    awarding_office_code = models.TextField()
    awarding_office_name = models.TextField()
    base_and_all_options_value = models.DecimalField(max_digits=15, decimal_places=2)
    business_funds_indicator = models.TextField()
    business_types = models.TextField()
    business_types_description = models.TextField()
    cfda_number = models.TextField()
    cfda_title = models.TextField()
    contract_award_type = models.TextField()
    contract_award_type_desc = models.TextField()
    extent_compete_description = models.TextField()
    extent_competed = models.TextField()
    fain = models.TextField()
    federal_action_obligation = models.DecimalField(max_digits=15, decimal_places=2)
    funding_agency_code = models.TextField()
    funding_agency_name = models.TextField()
    funding_sub_tier_agency_co = models.TextField()
    funding_sub_tier_agency_na = models.TextField()
    funding_office_code = models.TextField()
    funding_office_name = models.TextField()
    idv_type = models.TextField()
    idv_type_description = models.TextField()
    last_modified_date = models.TextField()
    naics = models.TextField()
    naics_description = models.TextField()
    original_loan_subsidy_cost = models.DecimalField(max_digits=15, decimal_places=2)
    parent_award_piid = models.TextField()
    period_of_performance_curr = models.DateTimeField()
    period_of_performance_star = models.DateTimeField()
    piid = models.TextField()
    pop_city_name = models.TextField()
    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_state_code = models.TextField()
    pop_state_name = models.TextField()
    pop_congressional_code = models.TextField()
    pop_zip5 = models.TextField()
    product_or_service_co_desc = models.TextField()
    product_or_service_code = models.TextField()
    pulled_from = models.TextField()
    recipient_location_address_line1 = models.TextField()
    recipient_location_address_line2 = models.TextField()
    recipient_location_address_line3 = models.TextField()
    recipient_location_city_name = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_foreign_province = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    record_type = models.IntegerField()
    referenced_idv_agency_iden = models.TextField()
    referenced_idv_type = models.TextField()
    referenced_idv_type_desc = models.TextField()
    sai_number = models.TextField()
    transaction_number = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    type_set_aside_description = models.TextField()
    uri = models.TextField()

    class Meta:
        managed = False
        db_table = 'transaction_matview'
