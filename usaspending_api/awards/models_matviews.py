import warnings

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.search import SearchVectorField
from django.core.cache import CacheKeyWarning
from django.db import models

from usaspending_api.awards.models import Award, Subaward, TransactionNormalized


warnings.simplefilter("ignore", CacheKeyWarning)


class UniversalTransactionView(models.Model):
    keyword_ts_vector = SearchVectorField()
    award_ts_vector = SearchVectorField()
    recipient_name_ts_vector = SearchVectorField()
    transaction = models.OneToOneField(TransactionNormalized, primary_key=True)
    action_date = models.DateField(blank=True, null=False)
    last_modified_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    action_type = models.TextField()
    award_id = models.IntegerField()
    award_category = models.TextField()
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    total_loan_value = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    total_obl_bin = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    piid = models.TextField()
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    transaction_description = models.TextField()
    modification_number = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_city_name = models.TextField()

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

    recipient_id = models.IntegerField()
    recipient_hash = models.UUIDField()
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
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    total_obl_bin = models.TextField()
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(
        max_digits=23, db_index=True, decimal_places=2, blank=True,
        null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_city_name = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_state_code = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()

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

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_categories = ArrayField(models.TextField(), default=list)
    cfda_number = models.TextField()
    cfda_title = models.TextField()
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
    keyword_ts_vector = SearchVectorField()
    award_ts_vector = SearchVectorField()
    recipient_name_ts_vector = SearchVectorField()
    award = models.OneToOneField(Award, primary_key=True)
    category = models.TextField()
    type = models.TextField()
    type_description = models.TextField()
    piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    description = models.TextField()
    total_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    total_loan_value = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    total_obl_bin = models.TextField()

    recipient_id = models.IntegerField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_categories = ArrayField(models.TextField(), default=list)

    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    last_modified_date = models.TextField()

    period_of_performance_start_date = models.DateField()
    period_of_performance_current_end_date = models.DateField()
    date_signed = models.DateField()
    ordering_period_end_date = models.DateField(null=True)

    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()

    awarding_toptier_agency_code = models.TextField()
    funding_toptier_agency_code = models.TextField()
    awarding_subtier_agency_code = models.TextField()
    funding_subtier_agency_code = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_city_name = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_city_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()

    cfda_number = models.TextField()
    sai_number = models.TextField()
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
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
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
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_award_view'


class SummaryView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
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

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view'


class SummaryNaicsCodesView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    naics_code = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_naics_codes'


class SummaryPscCodesView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    product_or_service_code = models.TextField(blank=True, null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_psc_codes'


class SummaryCfdaNumbersView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    pulled_from = models.TextField()
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_view_cfda_number'


class SummaryTransactionMonthView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
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
    recipient_location_city_name = models.TextField()

    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_state_code = models.TextField()
    pop_county_name = models.TextField()
    pop_county_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()

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

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_categories = ArrayField(models.TextField(), default=list)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)

    total_obl_bin = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_month_view'


class SummaryTransactionGeoView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
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
    recipient_location_city_name = models.TextField()

    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_state_code = models.TextField()
    pop_county_name = models.TextField()
    pop_county_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()

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

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_geo_view'


class SummaryStateView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()
    pulled_from = models.TextField()
    distinct_awards = ArrayField(models.TextField(), default=list)

    pop_country_code = models.TextField()
    pop_state_code = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_state_view'


class AwardCategory(models.Model):
    type_code = models.TextField(primary_key=True)
    type_name = models.TextField()

    class Meta:
        managed = False
        db_table = 'award_category'


class SubawardView(models.Model):
    subaward = models.OneToOneField(Subaward, primary_key=True, on_delete=models.deletion.DO_NOTHING)
    keyword_ts_vector = SearchVectorField()
    award_ts_vector = SearchVectorField()
    recipient_name_ts_vector = SearchVectorField()
    latest_transaction_id = models.IntegerField()
    last_modified_date = models.DateField()
    subaward_number = models.TextField()
    amount = models.DecimalField(max_digits=23, decimal_places=2)
    total_obl_bin = models.TextField()
    description = models.TextField(null=True, blank=True)
    fiscal_year = models.IntegerField()
    action_date = models.DateField()
    award_report_fy_month = models.IntegerField()
    award_report_fy_year = models.IntegerField()

    award = models.OneToOneField(Award, null=True)
    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

    recipient_unique_id = models.TextField()
    recipient_name = models.TextField()
    dba_name = models.TextField()
    parent_recipient_unique_id = models.TextField()
    parent_recipient_name = models.TextField()
    business_type_code = models.TextField()
    business_type_description = models.TextField()

    award_type = models.TextField()
    prime_award_type = models.TextField()

    cfda_id = models.IntegerField()
    piid = models.TextField()
    fain = models.TextField()

    business_categories = ArrayField(models.TextField(), default=list)
    prime_recipient_name = models.TextField()

    pulled_from = models.TextField()
    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()
    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    cfda_number = models.TextField()
    cfda_title = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_city_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_street_address = models.TextField()
    recipient_location_congressional_code = models.TextField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_state_name = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_city_code = models.TextField()
    pop_city_name = models.TextField()
    pop_zip5 = models.TextField()
    pop_street_address = models.TextField()
    pop_congressional_code = models.TextField()

    class Meta:
        managed = False
        db_table = 'subaward_view'


class SummaryTransactionRecipientView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()
    pulled_from = models.TextField()

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_recipient_view'


class SummaryTransactionFedAcctView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()
    pulled_from = models.TextField()

    federal_account_id = models.IntegerField()
    treasury_account_id = models.IntegerField()
    agency_identifier = models.TextField()
    main_account_code = models.TextField()
    account_title = models.TextField()
    federal_account_display = models.TextField()

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'summary_transaction_fed_acct_view'
