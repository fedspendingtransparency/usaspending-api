from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.search import SearchVectorField
from django.db import models

from usaspending_api.awards.models import TransactionNormalized


class UniversalTransactionView(models.Model):
    keyword_ts_vector = SearchVectorField()
    award_ts_vector = SearchVectorField()
    recipient_name_ts_vector = SearchVectorField()
    treasury_account_identifiers = ArrayField(models.IntegerField(), default=None)
    transaction = models.OneToOneField(TransactionNormalized, primary_key=True)
    action_date = models.DateField(blank=True, null=False)
    last_modified_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    action_type = models.TextField()
    award_id = models.BigIntegerField()
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
        db_table = "universal_transaction_matview"
