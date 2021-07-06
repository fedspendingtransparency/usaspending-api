from django.contrib.postgres.fields import ArrayField, JSONField
from django.db import models
from django.db.models import Q


class AbstractTransactionSearch(models.Model):
    transaction = models.OneToOneField("awards.TransactionNormalized", on_delete=models.DO_NOTHING, primary_key=True)
    award = models.ForeignKey("awards.Award", on_delete=models.DO_NOTHING, null=True)
    modification_number = models.TextField(null=True)
    detached_award_proc_unique = models.TextField(null=True)
    afa_generated_unique = models.TextField(null=True)
    generated_unique_award_id = models.TextField(null=True)
    fain = models.TextField(null=True)
    uri = models.TextField(null=True)
    piid = models.TextField(null=True)

    action_date = models.DateField(null=True)
    fiscal_action_date = models.DateField(null=True)
    last_modified_date = models.DateField(null=True)

    fiscal_year = models.IntegerField(null=True)
    award_certified_date = models.DateField(null=True)
    award_fiscal_year = models.IntegerField(null=True)
    update_date = models.DateTimeField(null=True)
    award_update_date = models.DateTimeField(null=True)
    etl_update_date = models.DateTimeField(null=True)
    period_of_performance_start_date = models.DateField(null=True)
    period_of_performance_current_end_date = models.DateField(null=True)

    type = models.TextField(null=True)
    type_description = models.TextField(null=True)
    award_category = models.TextField(null=True)
    transaction_description = models.TextField(null=True)
    award_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    business_categories = ArrayField(models.TextField(), null=True)
    naics_code = models.TextField(null=True)
    naics_description = models.TextField(null=True)
    product_or_service_code = models.TextField(null=True)
    product_or_service_description = models.TextField(null=True)
    type_of_contract_pricing = models.TextField(null=True)
    type_set_aside = models.TextField(null=True)
    extent_competed = models.TextField(null=True)
    ordering_period_end_date = models.TextField(null=True)
    cfda_number = models.TextField(null=True)
    cfda_title = models.TextField(null=True)
    cfda_id = models.IntegerField(null=True)

    pop_country_name = models.TextField(null=True)
    pop_country_code = models.TextField(null=True)
    pop_state_name = models.TextField(null=True)
    pop_state_code = models.TextField(null=True)
    pop_county_code = models.TextField(null=True)
    pop_county_name = models.TextField(null=True)
    pop_zip5 = models.TextField(null=True)
    pop_congressional_code = models.TextField(null=True)
    pop_congressional_population = models.IntegerField(null=True)
    pop_county_population = models.IntegerField(null=True)
    pop_state_fips = models.TextField(null=True)
    pop_state_population = models.IntegerField(null=True)
    pop_city_name = models.TextField(null=True)

    recipient_location_country_code = models.TextField(null=True)
    recipient_location_country_name = models.TextField(null=True)
    recipient_location_state_name = models.TextField(null=True)
    recipient_location_state_code = models.TextField(null=True)
    recipient_location_state_fips = models.TextField(null=True)
    recipient_location_state_population = models.IntegerField(null=True)
    recipient_location_county_code = models.TextField(null=True)
    recipient_location_county_name = models.TextField(null=True)
    recipient_location_county_population = models.IntegerField(null=True)
    recipient_location_congressional_code = models.TextField(null=True)
    recipient_location_congressional_population = models.IntegerField(null=True)
    recipient_location_zip5 = models.TextField(null=True)
    recipient_location_city_name = models.TextField(null=True)

    recipient_hash = models.UUIDField(null=True)
    recipient_levels = ArrayField(models.TextField(), null=True)
    recipient_name = models.TextField(null=True)
    recipient_unique_id = models.TextField(null=True)
    parent_recipient_hash = models.UUIDField(null=True)
    parent_recipient_name = models.TextField(null=True)
    parent_recipient_unique_id = models.TextField(null=True)

    awarding_toptier_agency_id = models.IntegerField(null=True)
    funding_toptier_agency_id = models.IntegerField(null=True)
    awarding_agency_id = models.IntegerField(null=True)
    funding_agency_id = models.IntegerField(null=True)
    awarding_toptier_agency_name = models.TextField(null=True)
    funding_toptier_agency_name = models.TextField(null=True)
    awarding_subtier_agency_name = models.TextField(null=True)
    funding_subtier_agency_name = models.TextField(null=True)
    awarding_office_code = models.TextField(null=True)
    awarding_office_name = models.TextField(null=True)
    funding_office_code = models.TextField(null=True)
    funding_office_name = models.TextField(null=True)
    awarding_toptier_agency_abbreviation = models.TextField(null=True)
    funding_toptier_agency_abbreviation = models.TextField(null=True)
    awarding_subtier_agency_abbreviation = models.TextField(null=True)
    funding_subtier_agency_abbreviation = models.TextField(null=True)

    treasury_account_identifiers = ArrayField(models.IntegerField(), null=True)
    tas_paths = ArrayField(models.TextField(), null=True)
    tas_components = ArrayField(models.TextField(), null=True)
    federal_accounts = JSONField(null=True)
    disaster_emergency_fund_codes = ArrayField(models.TextField(), null=True)

    class Meta:
        abstract = True


class TransactionSearch(AbstractTransactionSearch):
    """
    Fields in this model have all, with the exception of primary/foreign keys, been made nullable because it
    is directly populated by the contents of a materialized view. The fields used to create the materialized view
    may or may not be nullable, but those constraints are not enforced in this table.
    """

    class Meta:
        db_table = "transaction_search"
        indexes = [
            models.Index(fields=["transaction"], name="ts_idx_transaction_id"),
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
        ]
