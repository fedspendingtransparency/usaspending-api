import warnings
from django.db import models
from django.core.cache import CacheKeyWarning
from django.contrib.postgres.fields import ArrayField

warnings.simplefilter("ignore", CacheKeyWarning)


class UniversalTransactionView(models.Model):
    keyword_string = models.TextField()
    award_id_string = models.TextField()
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

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
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
