from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.db import models
from django.db.models import F, Q
from django.db.models.functions import Upper


class SubawardSearch(models.Model):
    # Broker Subaward Table Meta
    broker_created_at = models.DateTimeField(null=True, blank=True, db_index=True)
    broker_updated_at = models.DateTimeField(null=True, blank=True, db_index=True)
    broker_subaward_id = models.BigIntegerField(primary_key=True, db_index=True, unique=True)

    # Prime Award Fields (from Broker)
    unique_award_key = models.TextField(null=True, blank=True, db_index=True)
    award_piid_fain = models.TextField(null=True, blank=True)
    parent_award_id = models.TextField(null=True, blank=True)
    award_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    action_date = models.DateField(blank=True, null=True)
    fy = models.TextField(null=True, blank=True)
    awarding_agency_code = models.TextField(null=True, blank=True)
    awarding_agency_name = models.TextField(null=True, blank=True)
    awarding_sub_tier_agency_c = models.TextField(null=True, blank=True)
    awarding_sub_tier_agency_n = models.TextField(null=True, blank=True)
    awarding_office_code = models.TextField(null=True, blank=True)
    awarding_office_name = models.TextField(null=True, blank=True)
    funding_agency_code = models.TextField(null=True, blank=True)
    funding_agency_name = models.TextField(null=True, blank=True)
    funding_sub_tier_agency_co = models.TextField(null=True, blank=True)
    funding_sub_tier_agency_na = models.TextField(null=True, blank=True)
    funding_office_code = models.TextField(null=True, blank=True)
    funding_office_name = models.TextField(null=True, blank=True)
    awardee_or_recipient_uniqu = models.TextField(null=True, blank=True)
    awardee_or_recipient_uei = models.TextField(null=True, blank=True)
    awardee_or_recipient_legal = models.TextField(null=True, blank=True)
    dba_name = models.TextField(null=True, blank=True)
    ultimate_parent_unique_ide = models.TextField(null=True, blank=True)
    ultimate_parent_uei = models.TextField(null=True, blank=True)
    ultimate_parent_legal_enti = models.TextField(null=True, blank=True)
    legal_entity_country_code = models.TextField(null=True, blank=True)
    legal_entity_country_name = models.TextField(null=True, blank=True)
    legal_entity_state_code = models.TextField(null=True, blank=True)
    legal_entity_state_name = models.TextField(null=True, blank=True)
    legal_entity_zip = models.TextField(null=True, blank=True)
    legal_entity_county_code = models.TextField(null=True, blank=True)
    legal_entity_county_name = models.TextField(null=True, blank=True)
    legal_entity_congressional = models.TextField(null=True, blank=True)
    legal_entity_foreign_posta = models.TextField(null=True, blank=True)
    legal_entity_city_name = models.TextField(null=True, blank=True)
    legal_entity_address_line1 = models.TextField(null=True, blank=True)
    business_types = models.TextField(null=True, blank=True)
    place_of_perform_country_co = models.TextField(null=True, blank=True)
    place_of_perform_country_na = models.TextField(null=True, blank=True)
    place_of_perform_state_code = models.TextField(null=True, blank=True)
    place_of_perform_state_name = models.TextField(null=True, blank=True)
    place_of_performance_zip = models.TextField(null=True, blank=True)
    place_of_perform_county_code = models.TextField(null=True, blank=True)
    place_of_perform_county_name = models.TextField(null=True, blank=True)
    place_of_perform_congressio = models.TextField(null=True, blank=True)
    place_of_perform_city_name = models.TextField(null=True, blank=True)
    place_of_perform_street = models.TextField(null=True, blank=True)
    award_description = models.TextField(null=True, blank=True)
    naics = models.TextField(null=True, blank=True)
    naics_description = models.TextField(null=True, blank=True)
    cfda_numbers = models.TextField(null=True, blank=True)
    cfda_titles = models.TextField(null=True, blank=True)

    # Subaward Fields (from Broker)
    subaward_type = models.TextField(null=True, blank=True)
    subaward_report_year = models.SmallIntegerField(null=True, blank=True)
    subaward_report_month = models.SmallIntegerField(null=True, blank=True)
    subaward_number = models.TextField(null=True, blank=True)
    subaward_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    sub_action_date = models.DateField(blank=True, null=True)
    sub_awardee_or_recipient_uniqu = models.TextField(null=True, blank=True)
    sub_awardee_or_recipient_uei = models.TextField(null=True, blank=True)
    sub_awardee_or_recipient_legal_raw = models.TextField(null=True, blank=True)
    sub_dba_name = models.TextField(null=True, blank=True)
    sub_ultimate_parent_unique_ide = models.TextField(null=True, blank=True)
    sub_ultimate_parent_uei = models.TextField(null=True, blank=True)
    sub_ultimate_parent_legal_enti_raw = models.TextField(null=True, blank=True)
    sub_legal_entity_country_code_raw = models.TextField(null=True, blank=True)
    sub_legal_entity_country_name_raw = models.TextField(null=True, blank=True)
    sub_legal_entity_state_code = models.TextField(null=True, blank=True)
    sub_legal_entity_state_name = models.TextField(null=True, blank=True)
    sub_legal_entity_zip = models.TextField(null=True, blank=True)
    sub_legal_entity_county_code = models.TextField(null=True, blank=True)
    sub_legal_entity_county_name = models.TextField(null=True, blank=True)
    sub_legal_entity_congressional_raw = models.TextField(null=True, blank=True)
    sub_legal_entity_foreign_posta = models.TextField(null=True, blank=True)
    sub_legal_entity_city_name = models.TextField(null=True, blank=True)
    sub_legal_entity_address_line1 = models.TextField(null=True, blank=True)
    sub_business_types = models.TextField(null=True, blank=True)
    sub_place_of_perform_country_co_raw = models.TextField(null=True, blank=True)
    sub_place_of_perform_country_na = models.TextField(null=True, blank=True)
    sub_place_of_perform_state_code = models.TextField(null=True, blank=True)
    sub_place_of_perform_state_name = models.TextField(null=True, blank=True)
    sub_place_of_performance_zip = models.TextField(null=True, blank=True)
    sub_place_of_perform_county_code = models.TextField(null=True, blank=True)
    sub_place_of_perform_county_name = models.TextField(null=True, blank=True)
    sub_place_of_perform_congressio_raw = models.TextField(null=True, blank=True)
    sub_place_of_perform_city_name = models.TextField(null=True, blank=True)
    sub_place_of_perform_street = models.TextField(null=True, blank=True)
    subaward_description = models.TextField(null=True, blank=True)
    sub_high_comp_officer1_full_na = models.TextField(null=True, blank=True)
    sub_high_comp_officer1_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    sub_high_comp_officer2_full_na = models.TextField(null=True, blank=True)
    sub_high_comp_officer2_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    sub_high_comp_officer3_full_na = models.TextField(null=True, blank=True)
    sub_high_comp_officer3_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    sub_high_comp_officer4_full_na = models.TextField(null=True, blank=True)
    sub_high_comp_officer4_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    sub_high_comp_officer5_full_na = models.TextField(null=True, blank=True)
    sub_high_comp_officer5_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)

    # Additional Prime Award Fields (from Broker)
    prime_id = models.IntegerField(null=True, blank=True, db_index=True)
    internal_id = models.TextField(null=True, blank=True, db_index=True)
    date_submitted = models.DateTimeField(null=True, blank=True)
    report_type = models.TextField(null=True, blank=True)
    transaction_type = models.TextField(null=True, blank=True)
    program_title = models.TextField(null=True, blank=True)
    contract_agency_code = models.TextField(null=True, blank=True)
    contract_idv_agency_code = models.TextField(null=True, blank=True)
    grant_funding_agency_id = models.TextField(null=True, blank=True)
    grant_funding_agency_name = models.TextField(null=True, blank=True)
    federal_agency_name = models.TextField(null=True, blank=True)
    treasury_symbol = models.TextField(null=True, blank=True)
    dunsplus4 = models.TextField(null=True, blank=True)
    recovery_model_q1 = models.BooleanField(null=True, blank=True)
    recovery_model_q2 = models.BooleanField(null=True, blank=True)
    compensation_q1 = models.BooleanField(null=True, blank=True)
    compensation_q2 = models.BooleanField(null=True, blank=True)
    high_comp_officer1_full_na = models.TextField(null=True, blank=True)
    high_comp_officer1_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    high_comp_officer2_full_na = models.TextField(null=True, blank=True)
    high_comp_officer2_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    high_comp_officer3_full_na = models.TextField(null=True, blank=True)
    high_comp_officer3_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    high_comp_officer4_full_na = models.TextField(null=True, blank=True)
    high_comp_officer4_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    high_comp_officer5_full_na = models.TextField(null=True, blank=True)
    high_comp_officer5_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)

    # Additional Subaward Fields (from Broker)
    sub_id = models.IntegerField(null=True, blank=True)
    sub_parent_id = models.IntegerField(null=True, blank=True)
    sub_federal_agency_id = models.TextField(null=True, blank=True)
    sub_federal_agency_name = models.TextField(null=True, blank=True)
    sub_funding_agency_id = models.TextField(null=True, blank=True)
    sub_funding_agency_name = models.TextField(null=True, blank=True)
    sub_funding_office_id = models.TextField(null=True, blank=True)
    sub_funding_office_name = models.TextField(null=True, blank=True)
    sub_naics = models.TextField(null=True, blank=True)
    sub_cfda_numbers = models.TextField(null=True, blank=True)
    sub_dunsplus4 = models.TextField(null=True, blank=True)
    sub_recovery_subcontract_amt = models.TextField(null=True, blank=True)
    sub_recovery_model_q1 = models.BooleanField(null=True, blank=True)
    sub_recovery_model_q2 = models.BooleanField(null=True, blank=True)
    sub_compensation_q1 = models.BooleanField(null=True, blank=True)
    sub_compensation_q2 = models.BooleanField(null=True, blank=True)

    # USAS Links (and associated derivations)
    award = models.ForeignKey("search.AwardSearch", models.DO_NOTHING, related_name="subawardsearch", null=True)
    prime_award_group = models.TextField(null=True, blank=True)
    prime_award_type = models.TextField(null=True, blank=True)
    piid = models.TextField(null=True, blank=True)
    fain = models.TextField(null=True, blank=True)
    latest_transaction = models.ForeignKey(
        "search.TransactionSearch",
        on_delete=models.DO_NOTHING,
        related_name="subawardsearch",
        null=True,
        help_text="The latest transaction for the prime award by action_date and mod",
        db_constraint=False,
    )
    last_modified_date = models.DateField(null=True, blank=True)

    awarding_agency = models.ForeignKey(
        "references.Agency", models.DO_NOTHING, related_name="awarding_subawardsearch", null=True
    )
    awarding_toptier_agency_name = models.TextField(null=True, blank=True)
    awarding_toptier_agency_abbreviation = models.TextField(null=True, blank=True)
    awarding_subtier_agency_name = models.TextField(null=True, blank=True)
    awarding_subtier_agency_abbreviation = models.TextField(null=True, blank=True)

    funding_agency = models.ForeignKey(
        "references.Agency", models.DO_NOTHING, related_name="funding_subawardsearch", null=True
    )
    funding_toptier_agency_name = models.TextField(null=True, blank=True)
    funding_toptier_agency_abbreviation = models.TextField(null=True, blank=True)
    funding_subtier_agency_name = models.TextField(null=True, blank=True)
    funding_subtier_agency_abbreviation = models.TextField(null=True, blank=True)

    cfda = models.ForeignKey("references.Cfda", models.DO_NOTHING, related_name="related_subawardsearch", null=True)
    cfda_number = models.TextField(null=True, blank=True)
    cfda_title = models.TextField(null=True, blank=True)

    # USAS Derived Fields
    sub_fiscal_year = models.IntegerField()
    sub_total_obl_bin = models.TextField()
    sub_awardee_or_recipient_legal = models.TextField(null=True, blank=True)
    sub_ultimate_parent_legal_enti = models.TextField(null=True, blank=True)
    business_type_code = models.TextField(null=True, blank=True)
    business_categories = ArrayField(models.TextField(), default=list, null=True)
    treasury_account_identifiers = ArrayField(models.IntegerField(), default=None, null=True)
    pulled_from = models.TextField(null=True, blank=True)
    type_of_contract_pricing = models.TextField(null=True, blank=True)
    type_set_aside = models.TextField(null=True, blank=True)
    extent_competed = models.TextField(null=True, blank=True)
    product_or_service_code = models.TextField(null=True, blank=True)
    product_or_service_description = models.TextField(null=True, blank=True)

    legal_entity_congressional_current = models.TextField(null=True, blank=True)
    sub_legal_entity_country_code = models.TextField(null=True, blank=True)
    sub_legal_entity_country_name = models.TextField(null=True, blank=True)
    sub_legal_entity_zip5 = models.TextField(null=True, blank=True)
    sub_legal_entity_city_code = models.TextField(null=True, blank=True)
    sub_legal_entity_congressional = models.TextField(null=True, blank=True)
    sub_legal_entity_congressional_current = models.TextField(null=True, blank=True)

    place_of_performance_congressional_current = models.TextField(null=True, blank=True)
    place_of_perform_scope = models.TextField(null=True, blank=True)
    sub_place_of_perform_country_co = models.TextField(null=True, blank=True)
    sub_place_of_perform_country_name = models.TextField(null=True, blank=True)
    sub_place_of_perform_zip5 = models.TextField(null=True, blank=True)
    sub_place_of_perform_city_code = models.TextField(null=True, blank=True)
    sub_place_of_perform_congressio = models.TextField(null=True, blank=True)
    sub_place_of_performance_congressional_current = models.TextField(null=True, blank=True)
    legal_entity_state_fips = models.TextField(null=True, blank=True)
    place_of_perform_state_fips = models.TextField(null=True, blank=True)
    legal_entity_county_fips = models.TextField(null=True, blank=True)
    place_of_perform_county_fips = models.TextField(null=True, blank=True)
    pop_county_name = models.TextField(null=True, blank=True)

    # USAS Vectors
    keyword_ts_vector = SearchVectorField(null=True)
    award_ts_vector = SearchVectorField(null=True)
    recipient_name_ts_vector = SearchVectorField(null=True)

    program_activities = models.JSONField(null=True)
    prime_award_recipient_id = models.TextField(null=True, blank=True)
    subaward_recipient_hash = models.TextField(null=True, blank=True)
    subaward_recipient_level = models.TextField(null=True, blank=True)
    awarding_toptier_agency_code = models.TextField(null=True, blank=True)
    funding_toptier_agency_code = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "subaward_search"
        indexes = [
            models.Index(fields=["award_id"], name="ss_idx_award_id"),
            models.Index(
                fields=["prime_award_type"],
                name="ss_idx_prime_award_type",
                condition=Q(prime_award_type__isnull=False),
            ),
            models.Index(
                F("subaward_number").desc(nulls_last=True),
                name="ss_idx_ordered_subaward_number",
            ),
            models.Index(
                F("prime_award_type").desc(nulls_last=True),
                name="ss_idx_order_prime_award_type",
            ),
            models.Index(
                Upper("fain").desc(nulls_last=True),
                name="ss_idx_ordered_fain",
                condition=Q(fain__isnull=False),
            ),
            models.Index(
                Upper("piid").desc(nulls_last=True),
                name="ss_idx_ordered_piid",
                condition=Q(piid__isnull=False),
            ),
            models.Index(
                fields=["subaward_amount"],
                name="ss_idx_subaward_amount",
                condition=Q(subaward_amount__isnull=False),
            ),
            models.Index(
                F("subaward_amount").desc(nulls_last=True),
                name="ss_idx_ordered_subaward_amount",
            ),
            models.Index(fields=["sub_total_obl_bin"], name="ss_idx_sub_total_obl_bin"),
            GinIndex(
                fields=["sub_awardee_or_recipient_legal"], name="ss_idx_gin_sub_recp_name", opclasses=["gin_trgm_ops"]
            ),
            models.Index(
                fields=["sub_awardee_or_recipient_legal"],
                name="ss_idx_sub_recp_name",
                condition=Q(sub_awardee_or_recipient_legal__isnull=False),
            ),
            models.Index(
                fields=["sub_awardee_or_recipient_uniqu"],
                name="ss_idx_sub_duns",
                condition=Q(sub_awardee_or_recipient_uniqu__isnull=False),
            ),
            models.Index(
                fields=["sub_awardee_or_recipient_uei"],
                name="ss_idx_sub_uei",
                condition=Q(sub_awardee_or_recipient_uei__isnull=False),
            ),
            models.Index(
                fields=["sub_ultimate_parent_unique_ide"],
                name="ss_idx_sub_parent_duns",
                condition=Q(sub_ultimate_parent_unique_ide__isnull=False),
            ),
            models.Index(
                fields=["sub_ultimate_parent_uei"],
                name="ss_idx_sub_parent_uei",
                condition=Q(sub_ultimate_parent_uei__isnull=False),
            ),
            models.Index(
                F("sub_action_date").desc(nulls_last=True),
                name="ss_idx_sub_action_date",
            ),
            models.Index(
                F("last_modified_date").desc(nulls_last=True),
                name="ss_idx_last_modified_date",
            ),
            models.Index(
                F("sub_fiscal_year").desc(nulls_last=True),
                name="ss_idx_sub_fiscal_year",
            ),
            models.Index(
                F("awarding_agency_id").asc(nulls_last=True),
                name="ss_idx_awarding_agency_id",
                condition=Q(awarding_agency_id__isnull=False),
            ),
            models.Index(
                F("funding_agency_id").asc(nulls_last=True),
                name="ss_idx_funding_agency_id",
                condition=Q(funding_agency_id__isnull=False),
            ),
            models.Index(
                F("awarding_toptier_agency_name").desc(nulls_last=True),
                name="ss_idx_order_awarding_top_name",
            ),
            models.Index(
                F("awarding_subtier_agency_name").desc(nulls_last=True),
                name="ss_idx_order_awarding_sub_name",
            ),
            models.Index(
                fields=["awarding_toptier_agency_name"],
                name="ss_idx_awarding_top_agency_nam",
                condition=Q(awarding_toptier_agency_name__isnull=False),
            ),
            models.Index(
                fields=["awarding_subtier_agency_name"],
                name="ss_idx_awarding_sub_agency_nam",
                condition=Q(awarding_subtier_agency_name__isnull=False),
            ),
            models.Index(
                fields=["funding_toptier_agency_name"],
                name="ss_idx_funding_top_agency_name",
                condition=Q(funding_toptier_agency_name__isnull=False),
            ),
            models.Index(
                fields=["funding_subtier_agency_name"],
                name="ss_idx_funding_sub_agency_name",
                condition=Q(funding_subtier_agency_name__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_country_code"],
                name="ss_idx_sub_le_country_code",
                condition=Q(sub_legal_entity_country_code__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_state_code"],
                name="ss_idx_sub_le_state_code",
                condition=Q(sub_legal_entity_state_code__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_county_code"],
                name="ss_idx_sub_le_county_code",
                condition=Q(sub_legal_entity_county_code__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_zip5"],
                name="ss_idx_sub_le_zip5",
                condition=Q(sub_legal_entity_zip5__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_congressional"],
                name="ss_idx_sub_le_congressional",
                condition=Q(sub_legal_entity_congressional__isnull=False),
            ),
            models.Index(
                fields=["sub_legal_entity_city_name"],
                name="ss_idx_sub_le_city_name",
                condition=Q(sub_legal_entity_city_name__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_country_co"],
                name="ss_idx_sub_ppop_country_co",
                condition=Q(sub_place_of_perform_country_co__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_state_code"],
                name="ss_idx_sub_ppop_state_code",
                condition=Q(sub_place_of_perform_state_code__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_county_code"],
                name="ss_idx_sub_ppop_county_code",
                condition=Q(sub_place_of_perform_county_code__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_zip5"],
                name="ss_idx_sub_ppop_zip5",
                condition=Q(sub_place_of_perform_zip5__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_congressio"],
                name="ss_idx_sub_ppop_congressio",
                condition=Q(sub_place_of_perform_congressio__isnull=False),
            ),
            models.Index(
                fields=["sub_place_of_perform_city_name"],
                name="ss_idx_sub_ppop_city_name",
                condition=Q(sub_place_of_perform_city_name__isnull=False),
            ),
            models.Index(
                fields=["cfda_number"],
                name="ss_idx_cfda_number",
                condition=Q(cfda_number__isnull=False),
            ),
            models.Index(
                fields=["type_of_contract_pricing"],
                name="ss_idx_type_of_contract_pricin",
                condition=Q(type_of_contract_pricing__isnull=False),
            ),
            models.Index(
                fields=["extent_competed"],
                name="ss_idx_extent_competed",
                condition=Q(extent_competed__isnull=False),
            ),
            models.Index(
                fields=["type_set_aside"],
                name="ss_idx_type_set_aside",
                condition=Q(type_set_aside__isnull=False),
            ),
            models.Index(
                fields=["product_or_service_code"],
                name="ss_idx_product_service_code",
                condition=Q(product_or_service_code__isnull=False),
            ),
            GinIndex(
                fields=["product_or_service_description"],
                name="ss_idx_gin_product_service_des",
                opclasses=["gin_trgm_ops"],
            ),
            GinIndex(
                fields=["business_categories"],
                name="ss_idx_gin_business_categories",
            ),
            GinIndex(
                fields=["keyword_ts_vector"],
                name="ss_idx_gin_keyword_ts_vector",
            ),
            GinIndex(
                fields=["award_ts_vector"],
                name="ss_idx_gin_award_ts_vector",
            ),
            GinIndex(
                fields=["recipient_name_ts_vector"],
                name="ss_idx_gin_recip_name_ts_vecto",
            ),
            GinIndex(
                fields=["treasury_account_identifiers"],
                name="ss_idx_gin_treasury_account_id",
                opclasses=["gin__int_ops"],
            ),
            models.Index(fields=["product_or_service_code", "sub_action_date"], name="ss_idx_comp_psc_sub_action_dat"),
            models.Index(fields=["cfda_number", "sub_action_date"], name="ss_idx_comp_cfda_sub_action_da"),
        ]
