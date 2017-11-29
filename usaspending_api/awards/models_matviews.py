import warnings
from django.db import models
from django.core.cache import CacheKeyWarning
from django.contrib.postgres.fields import ArrayField

warnings.simplefilter("ignore", CacheKeyWarning)


class UniversalTransactionView(models.Model):
    # Fields
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

    issued_date = models.TextField()
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
    psc_code = models.TextField()
    psc_description = models.TextField()

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


class UniversalAwardView(models.Model):
    award_id = models.IntegerField(blank=False, null=False, primary_key=True)
    category = models.TextField()
    latest_transaction_id = models.IntegerField()
    type = models.TextField()
    type_description = models.TextField()
    description = models.TextField()
    piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    total_obligation = models.DecimalField(
        max_digits=15, decimal_places=2, blank=True,
        null=True)
    period_of_performance_start_date = models.DateField()
    period_of_performance_current_end_date = models.DateField()
    date_signed = models.DateField()
    base_and_all_options_value = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True,
        null=True)
    recipient_id = models.IntegerField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    issued_date = models.DateField()
    business_categories = ArrayField(models.TextField(), default=list)
    issued_date_fiscal_year = models.IntegerField()
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
    awarding_agency_office_name = models.TextField()
    funding_agency_office_name = models.TextField()
    recipient_location_address_line1 = models.TextField()
    recipient_location_address_line2 = models.TextField()
    recipient_location_address_line3 = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_country_code = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_foreign_province = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_city_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()
    pop_city_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_state_name = models.TextField()
    pop_state_code = models.TextField()
    pop_foreign_province = models.TextField()
    pop_congressional_code = models.TextField()
    pop_county_name = models.TextField()
    pop_county_code = models.TextField()
    cfda_number = models.TextField()
    pulled_from = models.TextField()
    type_of_contract_pricing = models.TextField()
    parent_award_id = models.TextField()
    idv_type = models.TextField()
    type_of_idc = models.TextField()
    referenced_idv_agency_iden = models.TextField()
    multiple_or_single_award_i = models.TextField()
    solicitation_identifier = models.TextField()
    solicitation_procedures = models.TextField()
    number_of_offers_received = models.TextField()
    extent_competed = models.TextField()
    extent_compete_description = models.TextField()
    type_set_aside = models.TextField()
    type_set_aside_description = models.TextField()
    commercial_item_acqui_desc = models.TextField()
    commercial_item_test_progr = models.TextField()
    evaluated_preference_desc = models.TextField()
    fed_biz_opps_description = models.TextField()
    small_business_competitive = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField()
    naics_description = models.TextField()
    dod_claimant_program_code = models.TextField()
    program_system_or_equipmen = models.TextField()
    information_technolog_desc = models.TextField()
    sea_transportation_desc = models.TextField()
    clinger_cohen_act_planning = models.TextField()
    davis_bacon_act_descrip = models.TextField()
    service_contract_act_desc = models.TextField()
    walsh_healey_act = models.TextField()
    consolidated_contract = models.TextField()
    cost_or_pricing_data_desc = models.TextField()
    fair_opportunity_limi_desc = models.TextField()
    foreign_funding_desc = models.TextField()
    interagency_contract_desc = models.TextField()
    major_program = models.TextField()
    multi_year_contract = models.TextField()
    price_evaluation_adjustmen = models.TextField()
    program_acronym = models.TextField()
    purchase_card_as_payment_m = models.TextField()
    subcontracting_plan_desc = models.TextField()
    vendor_phone_number = models.TextField()
    vendor_fax_number = models.TextField()

    class Meta:
        managed = False
        db_table = 'universal_award_matview'


class SummaryAwardView(models.Model):

    # Fields
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    category = models.TextField(blank=True, null=True)
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_agency_abbr = models.TextField(blank=True, null=True)
    funding_agency_id = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_agency_abbr = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_award_view'


class SummaryView(models.Model):

    # Fields
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    awarding_agency_id = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_agency_abbr = models.TextField(blank=True, null=True)
    funding_agency_id = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_agency_abbr = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view'


class SumaryNaicsCodesView(models.Model):

    # Fields
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    naics = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_naics_codes'


class SumaryPscCodesView(models.Model):

    # Fields
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_psc_codes'


class SumaryCfdaNumbersView(models.Model):

    # Fields
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2,
                                                    blank=True, null=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_cfda_number'
