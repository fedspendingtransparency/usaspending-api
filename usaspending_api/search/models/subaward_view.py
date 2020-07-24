from django.contrib.postgres.fields import ArrayField
from django.db import models

from usaspending_api.awards.models import Award, Subaward, BrokerSubaward


class SubawardView(models.Model):
    subaward = models.OneToOneField(Subaward, primary_key=True, on_delete=models.deletion.DO_NOTHING)
    broker_subaward = models.OneToOneField(BrokerSubaward, on_delete=models.deletion.DO_NOTHING)
    treasury_account_identifiers = ArrayField(models.IntegerField(), default=None)
    latest_transaction_id = models.BigIntegerField()
    last_modified_date = models.DateField()
    subaward_number = models.TextField()
    amount = models.DecimalField(max_digits=23, decimal_places=2)
    total_obl_bin = models.TextField()
    description = models.TextField(null=True, blank=True)
    fiscal_year = models.IntegerField()
    action_date = models.DateField()
    award_report_fy_month = models.IntegerField()
    award_report_fy_year = models.IntegerField()

    award = models.OneToOneField(Award, on_delete=models.DO_NOTHING, null=True)
    generated_unique_award_id = models.TextField()
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
        db_table = "subaward_view"
