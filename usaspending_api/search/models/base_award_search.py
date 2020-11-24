from django.contrib.postgres.fields import ArrayField
from django.db import models

from usaspending_api.awards.models import Award


class BaseAwardSearchModel(models.Model):
    """This is an Abstract Base Class for a number of materialized view models

    The models which inherit this shouldn't include additional fields without
    careful consideration of the:
        - "Search View",
        - Django filter queryset logic for matviews
        - API views obtaining data from the matviews
    """

    treasury_account_identifiers = ArrayField(models.IntegerField(), default=None)
    award = models.OneToOneField(Award, on_delete=models.DO_NOTHING, primary_key=True, related_name="%(class)s")
    category = models.TextField()
    type = models.TextField()
    type_description = models.TextField()
    generated_unique_award_id = models.TextField()
    display_award_id = models.TextField()
    update_date = models.DateField()
    piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()
    award_amount = models.DecimalField(max_digits=23, decimal_places=2)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    description = models.TextField()
    total_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2)
    total_loan_value = models.DecimalField(max_digits=23, decimal_places=2)
    total_obl_bin = models.TextField()

    recipient_hash = models.UUIDField()
    recipient_levels = models.ArrayField(models.TextField(), default=list)
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

    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2)

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    funding_toptier_agency_id = models.IntegerField()
    funding_subtier_agency_id = models.IntegerField()
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
    recipient_location_state_name = models.TextField()
    recipient_location_state_fips = models.TextField()
    recipient_location_state_population = models.IntegerField()
    recipient_location_county_population = models.IntegerField()
    recipient_location_congressional_population = models.IntegerField()

    pop_country_code = models.TextField()
    pop_country_name = models.TextField()
    pop_state_code = models.TextField()
    pop_county_code = models.TextField()
    pop_county_name = models.TextField()
    pop_city_code = models.TextField()
    pop_zip5 = models.TextField()
    pop_congressional_code = models.TextField()
    pop_city_name = models.TextField()
    pop_state_name = models.TextField()
    pop_state_fips = models.TextField()
    pop_state_population = models.IntegerField()
    pop_county_population = models.IntegerField()
    pop_congressional_population = models.IntegerField()

    cfda_program_title = models.TextField()
    cfda_number = models.TextField()
    sai_number = models.TextField()
    type_of_contract_pricing = models.TextField()
    extent_competed = models.TextField()
    type_set_aside = models.TextField()

    product_or_service_code = models.TextField()
    product_or_service_description = models.TextField()
    naics_code = models.TextField()
    naics_description = models.TextField()

    tas_paths = ArrayField(models.TextField(), default=list)
    tas_components = ArrayField(models.TextField(), default=list)
    disaster_emergency_fund_codes = ArrayField(models.TextField(), default=list)
    total_covid_outlay = models.DecimalField(max_digits=23, decimal_places=2)
    total_covid_obligation = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        abstract = True
