from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.constraints import OpClass
from django.db import models
from django.db.models import F, Q
from django.db.models.functions import Upper
from django_cte import CTEManager

from usaspending_api.awards.models import Award


class AwardSearch(models.Model):
    treasury_account_identifiers = ArrayField(models.IntegerField(), default=list, null=True)
    award = models.OneToOneField(Award, on_delete=models.DO_NOTHING, primary_key=True, related_name="%(class)s")
    category = models.TextField(null=True, db_index=True)
    type_raw = models.TextField(null=True, db_index=True)
    type_description_raw = models.TextField(null=True)
    type = models.TextField(null=True, db_index=True)
    type_description = models.TextField(null=True)
    generated_unique_award_id = models.TextField(null=False, unique=True)
    generated_unique_award_id_legacy = models.TextField(
        null=True, unique=True, help_text="Legacy generated unique award ID built using subtier awarding agency code"
    )
    display_award_id = models.TextField(null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    piid = models.TextField(null=True, db_index=True)
    fain = models.TextField(null=True, db_index=True)
    uri = models.TextField(null=True, db_index=True)
    award_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True, db_index=True)
    total_outlays = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True, db_index=True)
    description = models.TextField(null=True)
    total_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_loan_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_obl_bin = models.TextField(null=True)

    recipient_hash = models.UUIDField(null=True)
    recipient_levels = ArrayField(models.TextField(), default=list, null=True)
    recipient_name = models.TextField(null=True)
    recipient_unique_id = models.TextField(null=True)
    parent_recipient_unique_id = models.TextField(null=True)
    recipient_uei = models.TextField(null=True, blank=True)
    parent_uei = models.TextField(null=True, blank=True)
    parent_recipient_name = models.TextField(null=True, blank=True)
    business_categories = ArrayField(models.TextField(), default=list, null=True)

    action_date = models.DateField(null=True)
    fiscal_year = models.IntegerField(null=True)
    last_modified_date = models.DateField(blank=True, null=True)

    period_of_performance_start_date = models.DateField(null=True, db_index=True)
    period_of_performance_current_end_date = models.DateField(null=True, db_index=True)
    date_signed = models.DateField(null=True)
    ordering_period_end_date = models.DateField(null=True)

    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    awarding_agency_id = models.IntegerField(null=True, db_index=True)
    funding_agency_id = models.IntegerField(null=True, db_index=True)
    funding_toptier_agency_id = models.IntegerField(null=True)
    funding_subtier_agency_id = models.IntegerField(null=True)
    awarding_toptier_agency_name = models.TextField(null=True)
    funding_toptier_agency_name = models.TextField(null=True)
    awarding_subtier_agency_name = models.TextField(null=True)
    funding_subtier_agency_name = models.TextField(null=True)

    awarding_toptier_agency_name_raw = models.TextField(null=True)
    funding_toptier_agency_name_raw = models.TextField(null=True)
    awarding_subtier_agency_name_raw = models.TextField(null=True)
    funding_subtier_agency_name_raw = models.TextField(null=True)

    awarding_toptier_agency_code = models.TextField(null=True)
    funding_toptier_agency_code = models.TextField(null=True)
    awarding_subtier_agency_code = models.TextField(null=True)
    funding_subtier_agency_code = models.TextField(null=True)

    awarding_toptier_agency_code_raw = models.TextField(null=True)
    funding_toptier_agency_code_raw = models.TextField(null=True)
    awarding_subtier_agency_code_raw = models.TextField(null=True)
    funding_subtier_agency_code_raw = models.TextField(null=True)

    federal_accounts = models.JSONField(null=True)

    recipient_location_country_code = models.TextField(null=True)
    recipient_location_country_name = models.TextField(null=True)
    recipient_location_state_code = models.TextField(null=True)
    recipient_location_county_code = models.TextField(null=True)
    recipient_location_county_name = models.TextField(null=True)
    recipient_location_zip5 = models.TextField(null=True)
    recipient_location_congressional_code = models.TextField(null=True)
    recipient_location_congressional_code_current = models.TextField(null=True)
    recipient_location_city_name = models.TextField(null=True)
    recipient_location_state_name = models.TextField(null=True)
    recipient_location_state_fips = models.TextField(null=True)
    recipient_location_state_population = models.IntegerField(null=True)
    recipient_location_county_population = models.IntegerField(null=True)
    recipient_location_congressional_population = models.IntegerField(null=True)
    recipient_location_county_fips = models.TextField(null=True)
    recipient_location_address_line1 = models.TextField(null=True)
    recipient_location_address_line2 = models.TextField(null=True)
    recipient_location_address_line3 = models.TextField(null=True)
    recipient_location_zip4 = models.TextField(null=True)
    recipient_location_foreign_postal_code = models.TextField(null=True)
    recipient_location_foreign_province = models.TextField(null=True)

    pop_country_code = models.TextField(null=True)
    pop_country_name = models.TextField(null=True)
    pop_state_code = models.TextField(null=True)
    pop_county_code = models.TextField(null=True)
    pop_county_name = models.TextField(null=True)
    pop_city_code = models.TextField(null=True)
    pop_zip5 = models.TextField(null=True)
    pop_congressional_code = models.TextField(null=True)
    pop_congressional_code_current = models.TextField(null=True)
    pop_city_name = models.TextField(null=True)
    pop_state_name = models.TextField(null=True)
    pop_state_fips = models.TextField(null=True)
    pop_state_population = models.IntegerField(null=True)
    pop_county_population = models.IntegerField(null=True)
    pop_congressional_population = models.IntegerField(null=True)
    pop_county_fips = models.TextField(null=True)
    pop_zip4 = models.TextField(null=True)

    cfda_program_title = models.TextField(null=True)
    cfda_number = models.TextField(null=True)
    cfdas = ArrayField(models.TextField(), default=list, null=True)
    sai_number = models.TextField(null=True)
    type_of_contract_pricing = models.TextField(null=True)
    extent_competed = models.TextField(null=True)
    type_set_aside = models.TextField(null=True)

    product_or_service_code = models.TextField(null=True)
    product_or_service_description = models.TextField(null=True)
    naics_code = models.TextField(null=True)
    naics_description = models.TextField(null=True)

    tas_paths = ArrayField(models.TextField(), default=list, null=True)
    tas_components = ArrayField(models.TextField(), default=list, null=True)

    disaster_emergency_fund_codes = ArrayField(models.TextField(), default=list, null=True)
    spending_by_defc = models.JSONField(null=True)
    total_covid_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_covid_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_iija_outlay = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_iija_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_1_name = models.TextField(null=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True)
    is_fpds = models.BooleanField(default=False)
    fpds_agency_id = models.TextField(null=True)
    fpds_parent_agency_id = models.TextField(null=True)
    base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_subaward_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    subaward_count = models.IntegerField(null=True)
    base_exercised_options_val = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    parent_award_piid = models.TextField(null=True, db_index=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(null=True, auto_now_add=True)
    total_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    latest_transaction = models.ForeignKey(
        "awards.TransactionNormalized",
        on_delete=models.DO_NOTHING,
        related_name="latest_for_award",
        null=True,
        help_text="The latest transaction by action_date and mod associated with this award",
        db_constraint=False,
    )
    earliest_transaction = models.ForeignKey(
        "awards.TransactionNormalized",
        on_delete=models.DO_NOTHING,
        related_name="earliest_for_award",
        null=True,
        help_text="The earliest transaction by action_date and mod associated with this award",
        db_constraint=False,
    )
    latest_transaction_search = models.ForeignKey(
        "search.TransactionSearch",
        on_delete=models.DO_NOTHING,
        related_name="latest_for_award",
        null=True,
        help_text="The latest transaction in transaction_search table by action_date and mod associated with this "
        "award",
        db_constraint=False,
    )
    earliest_transaction_search = models.ForeignKey(
        "search.TransactionSearch",
        on_delete=models.DO_NOTHING,
        related_name="earliest_for_award",
        null=True,
        help_text="The earliest transaction in transaction_search table by action_date and mod associated with this "
        "award",
        db_constraint=False,
    )
    total_indirect_federal_sharing = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    transaction_unique_id = models.TextField(null=True)
    raw_recipient_name = models.TextField(null=True)
    data_source = models.TextField(null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    program_activities = models.JSONField(null=True)

    objects = CTEManager()

    class Meta:
        db_table = "award_search"
        constraints = [
            models.UniqueConstraint(
                OpClass("generated_unique_award_id_legacy", name="text_pattern_ops"),
                name="as_idx_unique_award_id_legacy",
            )
        ]
        indexes = [
            models.Index(
                fields=["recipient_hash"], name="as_idx_recipient_hash", condition=Q(action_date__gte="2007-10-01")
            ),
            models.Index(
                fields=["recipient_unique_id"],
                name="as_idx_recipient_unique_id",
                condition=Q(recipient_unique_id__isnull=False) & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                F("action_date").desc(nulls_last=True),
                name="as_idx_action_date",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["funding_agency_id"],
                name="as_idx_funding_agency_id",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_congressional_code"],
                name="as_idx_recipient_cong_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_county_code"],
                name="as_idx_recipient_county_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_state_code"],
                name="as_idx_recipient_state_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            # mimicking transaction_search's indexes, this additional index accounts for pre-2008 data
            models.Index(
                F("action_date").desc(nulls_last=True),
                name="as_idx_action_date_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
            models.Index(Upper("piid"), name="as_idx_piid_upper"),
            models.Index(Upper("parent_award_piid"), name="as_idx_parent_award_piid_upper"),
            models.Index(Upper("fain"), name="as_idx_fain_upper"),
            models.Index(Upper("uri"), name="as_idx_uri_upper"),
            models.Index(F("update_date").desc(nulls_last=True), name="as_idx_update_date_desc"),
        ]
