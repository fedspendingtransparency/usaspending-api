import os

from django.db import models

from usaspending_api.common.custom_django_fields import NumericField
from usaspending_api.references.models import Cfda


class TransactionFABS(models.Model):
    """
    NOTE: Before adding new fields to this model, consider whether adding them to the TransactionSearch
    model would meet your needs. In the future, we'd like to completely refactor out the views built on
    top of TransactionSearch and AwardSearch. In the meantime, new fields should be added to these base
    models when possible to prevent more future rework."""

    transaction = models.OneToOneField(
        "awards.TransactionNormalized", on_delete=models.CASCADE, primary_key=True, related_name="assistance_data"
    )
    published_fabs_id = models.IntegerField(blank=True, null=True, db_index=True)
    afa_generated_unique = models.TextField(unique=True, null=False, db_index=True)
    action_date = models.TextField(blank=True, null=True)
    action_type = models.TextField(blank=True, null=True)
    action_type_description = models.TextField(blank=True, null=True)
    assistance_type = models.TextField(blank=True, null=True)
    assistance_type_desc = models.TextField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    business_funds_indicator = models.TextField(blank=True, null=True)
    business_funds_ind_desc = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    business_types_desc = models.TextField(blank=True, null=True)
    cfda_number = models.TextField(blank=True, null=True, db_index=True)
    cfda_title = models.TextField(blank=True, null=True)
    correction_delete_indicatr = models.TextField(blank=True, null=True)
    correction_delete_ind_desc = models.TextField(blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    fain = models.TextField(blank=True, null=True, db_index=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    funding_opportunity_goals = models.TextField(blank=True, null=True)
    funding_opportunity_number = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    indirect_federal_sharing = NumericField(blank=True, null=True)
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_city_code = models.TextField(blank=True, null=True)
    legal_entity_foreign_descr = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)
    recipient_location_congressional_code_current = models.TextField(null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    legal_entity_county_code = models.TextField(blank=True, null=True)
    legal_entity_county_name = models.TextField(blank=True, null=True)
    legal_entity_foreign_city = models.TextField(blank=True, null=True)
    legal_entity_foreign_posta = models.TextField(blank=True, null=True)
    legal_entity_foreign_provi = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_name = models.TextField(blank=True, null=True)
    legal_entity_zip5 = models.TextField(blank=True, null=True)
    legal_entity_zip_last4 = models.TextField(blank=True, null=True)
    modified_at = models.DateTimeField(blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    period_of_performance_curr = models.TextField(blank=True, null=True)
    period_of_performance_star = models.TextField(blank=True, null=True)
    place_of_performance_city = models.TextField(blank=True, null=True)
    place_of_performance_code = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)
    pop_congressional_code_current = models.TextField(null=True)
    place_of_perform_country_c = models.TextField(blank=True, null=True)
    place_of_perform_country_n = models.TextField(blank=True, null=True)
    place_of_perform_county_co = models.TextField(blank=True, null=True)
    place_of_perform_county_na = models.TextField(blank=True, null=True)
    place_of_performance_forei = models.TextField(blank=True, null=True)
    place_of_perform_state_nam = models.TextField(blank=True, null=True)
    place_of_perfor_state_code = models.TextField(blank=True, null=True)
    place_of_performance_zip4a = models.TextField(blank=True, null=True)
    place_of_performance_zip5 = models.TextField(blank=True, null=True)
    place_of_perform_zip_last4 = models.TextField(blank=True, null=True)
    place_of_performance_scope = models.TextField(blank=True, null=True)
    record_type = models.IntegerField(blank=True, null=True)
    record_type_description = models.TextField(blank=True, null=True)
    sai_number = models.TextField(blank=True, null=True)
    total_funding_amount = models.TextField(blank=True, null=True)
    uei = models.TextField(blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)
    ultimate_parent_uei = models.TextField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True, db_index=True)
    unique_award_key = models.TextField(null=True, db_index=True)
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    @property
    def cfda_objectives(self):
        cfda = Cfda.objects.filter(program_number=self.cfda_number).first()
        if cfda:
            return cfda.objectives

    class Meta:
        managed = False
        db_table = "vw_transaction_fabs"
        unique_together = (("awarding_sub_tier_agency_c", "award_modification_amendme", "fain", "uri", "cfda_number"),)


FABS_ALT_COL_NAMES_IN_TRANSACTION_SEARCH = {
    # transaction_fabs col name : transaction_search col name
    "award_modification_amendme": "modification_number",
    "unique_award_key": "generated_unique_award_id",
    "modified_at": "last_modified_date",
    "period_of_performance_star": "period_of_performance_start_date",
    "period_of_performance_curr": "period_of_performance_current_end_date",
    "awarding_agency_name": "awarding_toptier_agency_name",
    "funding_agency_name": "funding_toptier_agency_name",
    "awarding_sub_tier_agency_n": "awarding_subtier_agency_name",
    "funding_sub_tier_agency_na": "funding_subtier_agency_name",
    "assistance_type": "type",
    "assistance_type_desc": "type_description",
    "award_description": "transaction_description",
    "uei": "recipient_uei",
    "awardee_or_recipient_legal": "recipient_name",
    "awardee_or_recipient_uniqu": "recipient_unique_id",
    "ultimate_parent_uei": "parent_uei",
    "ultimate_parent_legal_enti": "parent_recipient_name",
    "ultimate_parent_unique_ide": "parent_recipient_unique_id",
    "legal_entity_country_code": "recipient_location_country_code",
    "legal_entity_country_name": "recipient_location_country_name",
    "legal_entity_state_code": "recipient_location_state_code",
    "legal_entity_state_name": "recipient_location_state_name",
    "legal_entity_county_code": "recipient_location_county_code",
    "legal_entity_county_name": "recipient_location_county_name",
    "legal_entity_congressional": "recipient_location_congressional_code",
    "legal_entity_zip5": "recipient_location_zip5",
    "legal_entity_city_name": "recipient_location_city_name",
    "place_of_perform_country_c": "pop_country_code",
    "place_of_perform_country_n": "pop_country_name",
    "place_of_perfor_state_code": "pop_state_code",
    "place_of_perform_state_nam": "pop_state_name",
    "place_of_perform_county_co": "pop_county_code",
    "place_of_perform_county_na": "pop_county_name",
    "place_of_performance_congr": "pop_congressional_code",
    "place_of_performance_zip5": "pop_zip5",
    "place_of_performance_city": "pop_city_name",
}

FABS_CASTED_COL_MAP = {
    # transaction_fabs col name : type casting search -> fabs
    "action_date": "TEXT",
    "modified_at": "TIMESTAMP WITH TIME ZONE",
    "period_of_performance_star": "TEXT",
    "period_of_performance_curr": "TEXT",
    "total_funding_amount": "TEXT",
}

FABS_TO_TRANSACTION_SEARCH_COL_MAP = {
    f.column: FABS_ALT_COL_NAMES_IN_TRANSACTION_SEARCH.get(f.column, f.column) for f in TransactionFABS._meta.fields
}

vw_transaction_fabs_sql = f"""
    CREATE OR REPLACE VIEW rpt.vw_transaction_fabs AS
        SELECT
            {(','+os.linesep+' '*12).join([
                (v+(f'::{FABS_CASTED_COL_MAP[k]}' if k in FABS_CASTED_COL_MAP else '')).ljust(62)+' AS '+k.ljust(48)
                for k, v in FABS_TO_TRANSACTION_SEARCH_COL_MAP.items()])}
        FROM
            rpt.transaction_search
        WHERE
            is_fpds = False;
"""
