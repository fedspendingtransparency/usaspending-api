from django.db import models
from django.contrib.postgres.fields import ArrayField

from usaspending_api.common.custom_django_fields import NumericField, NaiveTimestampField, BooleanFieldWithDefault


class SourceAssistanceTransaction(models.Model):
    """Raw assistance transaction record originating from Broker

    Model contains a 100% duplicate copy of *active*
    award modifications (aka transactions) stored in
    published_fabs from a Broker database.

    NO DATA MANIPULATION SHOULD BE PERFORMED BY DATA ETL

    Put non-null fields on the top, all other fields sort alphabetically
    """

    published_fabs_id = models.IntegerField(primary_key=True, help_text="surrogate primary key defined in Broker")
    afa_generated_unique = models.TextField(unique=True, help_text="natural key defined in Broker")
    action_date = models.TextField(blank=True, null=True)
    action_type = models.TextField(blank=True, null=True)
    action_type_description = models.TextField(blank=True, null=True)
    assistance_type = models.TextField(blank=True, null=True)
    assistance_type_desc = models.TextField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    business_categories = ArrayField(models.TextField(), default=None, null=True)
    business_funds_ind_desc = models.TextField(blank=True, null=True)
    business_funds_indicator = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    business_types_desc = models.TextField(blank=True, null=True)
    assistance_listing_number = models.TextField(blank=True, null=True, db_index=False)
    assistance_listing_title = models.TextField(blank=True, null=True)
    correction_delete_ind_desc = models.TextField(blank=True, null=True)
    correction_delete_indicatr = models.TextField(blank=True, null=True)
    created_at = NaiveTimestampField(
        help_text="record creation datetime in Broker", blank=True, null=True, db_index=True
    )
    face_value_loan_guarantee = NumericField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True, db_index=True)
    federal_action_obligation = NumericField(blank=True, null=True)
    fiscal_year_and_quarter_co = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    funding_opportunity_goals = models.TextField(blank=True, null=True)
    funding_opportunity_number = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    high_comp_officer1_amount = models.TextField(blank=True, null=True)
    high_comp_officer1_full_na = models.TextField(blank=True, null=True)
    high_comp_officer2_amount = models.TextField(blank=True, null=True)
    high_comp_officer2_full_na = models.TextField(blank=True, null=True)
    high_comp_officer3_amount = models.TextField(blank=True, null=True)
    high_comp_officer3_full_na = models.TextField(blank=True, null=True)
    high_comp_officer4_amount = models.TextField(blank=True, null=True)
    high_comp_officer4_full_na = models.TextField(blank=True, null=True)
    high_comp_officer5_amount = models.TextField(blank=True, null=True)
    high_comp_officer5_full_na = models.TextField(blank=True, null=True)
    indirect_federal_sharing = NumericField(blank=True, null=True)
    is_active = BooleanFieldWithDefault()
    is_historical = models.BooleanField(null=True, blank=True)
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_city_code = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    legal_entity_county_code = models.TextField(blank=True, null=True)
    legal_entity_county_name = models.TextField(blank=True, null=True)
    legal_entity_foreign_city = models.TextField(blank=True, null=True)
    legal_entity_foreign_descr = models.TextField(blank=True, null=True)
    legal_entity_foreign_posta = models.TextField(blank=True, null=True)
    legal_entity_foreign_provi = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_name = models.TextField(blank=True, null=True)
    legal_entity_zip5 = models.TextField(blank=True, null=True)
    legal_entity_zip_last4 = models.TextField(blank=True, null=True)
    modified_at = NaiveTimestampField(blank=True, null=True)
    non_federal_funding_amount = NumericField(blank=True, null=True)
    original_loan_subsidy_cost = NumericField(blank=True, null=True)
    period_of_performance_curr = models.TextField(blank=True, null=True)
    period_of_performance_star = models.TextField(blank=True, null=True)
    place_of_perfor_state_code = models.TextField(blank=True, null=True)
    place_of_perform_country_c = models.TextField(blank=True, null=True)
    place_of_perform_country_n = models.TextField(blank=True, null=True)
    place_of_perform_county_co = models.TextField(blank=True, null=True)
    place_of_perform_county_na = models.TextField(blank=True, null=True)
    place_of_perform_state_nam = models.TextField(blank=True, null=True)
    place_of_perform_zip_last4 = models.TextField(blank=True, null=True)
    place_of_performance_city = models.TextField(blank=True, null=True)
    place_of_performance_code = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)
    place_of_performance_forei = models.TextField(blank=True, null=True)
    place_of_performance_zip4a = models.TextField(blank=True, null=True)
    place_of_performance_zip5 = models.TextField(blank=True, null=True)
    place_of_performance_scope = models.TextField(blank=True, null=True)
    record_type = models.IntegerField(blank=True, null=True)
    record_type_description = models.TextField(blank=True, null=True)
    sai_number = models.TextField(blank=True, null=True)
    submission_id = NumericField(blank=True, null=True)
    total_funding_amount = models.TextField(blank=True, null=True)
    uei = models.TextField(blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)
    ultimate_parent_uei = models.TextField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(blank=True, null=True)
    unique_award_key = models.TextField(null=True, db_index=True)
    updated_at = NaiveTimestampField(
        help_text="record last update datetime in Broker", blank=True, null=True, db_index=True
    )
    uri = models.TextField(blank=True, null=True, db_index=True)

    class Meta:
        db_table = "source_assistance_transaction"
        # UNIQUE index also created on UPPER(afa_generated_unique)

    @property
    def table_name(self):
        return self._meta.db_table

    @property
    def broker_source_table(self):
        return "published_fabs"
