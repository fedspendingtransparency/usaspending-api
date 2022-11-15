from django.db import models

from usaspending_api.common.custom_django_fields import NumericField
from usaspending_api.references.models import Cfda


class TransactionFABS(models.Model):
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

    # Timestamp field auto generated by broker
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True, db_index=True)

    @property
    def cfda_objectives(self):
        cfda = Cfda.objects.filter(program_number=self.cfda_number).first()
        if cfda:
            return cfda.objectives

    class Meta:
        managed = False
        db_table = "vw_transaction_fabs"
        unique_together = (("awarding_sub_tier_agency_c", "award_modification_amendme", "fain", "uri", "cfda_number"),)


vw_transaction_fabs_sql = """
    CREATE OR REPLACE VIEW rpt.vw_transaction_fabs AS
        SELECT
            -- Keys
            transaction_id,
            published_fabs_id,
            modification_number                     AS "award_modification_amendme",
            generated_unique_award_id               AS "unique_award_key",
            -- Dates
            action_date,
            last_modified_date                      AS "modified_at",
            period_of_performance_start_date        AS "period_of_performance_star",
            period_of_performance_current_end_date  AS "period_of_performance_curr",
            -- Agencies
            awarding_agency_code,
            awarding_toptier_agency_name            AS "awarding_agency_name",
            funding_agency_code,
            funding_toptier_agency_name             AS "funding_agency_name",
            awarding_sub_tier_agency_c,
            awarding_subtier_agency_name            AS "awarding_sub_tier_agency_n",
            funding_sub_tier_agency_co,
            funding_subtier_agency_name             AS "funding_sub_tier_agency_na",
            awarding_office_code,
            awarding_office_name,
            funding_office_code,
            funding_office_name,
            -- Typing
            type                                    AS "assistance_type",
            type_description                        AS "assistance_type_desc",
            action_type,
            action_type_description,
            transaction_description                 AS "award_description",
            -- Amounts
            federal_action_obligation,
            original_loan_subsidy_cost,
            face_value_loan_guarantee,
            indirect_federal_sharing,
            total_funding_amount,
            non_federal_funding_amount,
            -- Recipient
            recipient_uei                           AS "uei",
            recipient_name                          AS "awardee_or_recipient_legal",
            recipient_unique_id                     AS "awardee_or_recipient_uniqu",
            parent_uei                              AS "ultimate_parent_uei",
            parent_recipient_name                   AS "ultimate_parent_legal_enti",
            parent_recipient_unique_id              AS "ultimate_parent_unique_ide",
            -- Recipient Location
            recipient_location_country_code         AS "legal_entity_country_code",
            recipient_location_country_name         AS "legal_entity_country_name",
            recipient_location_state_code           AS "legal_entity_state_code",
            recipient_location_state_name           AS "legal_entity_state_name",
            recipient_location_county_code          AS "legal_entity_county_code",
            recipient_location_county_name          AS "legal_entity_county_name",
            recipient_location_congressional_code   AS "legal_entity_congressional",
            recipient_location_zip5                 AS "legal_entity_zip5",
            legal_entity_zip_last4,
            legal_entity_city_code,
            recipient_location_city_name            AS "legal_entity_city_name",
            legal_entity_address_line1,
            legal_entity_address_line2,
            legal_entity_address_line3,
            legal_entity_foreign_city,
            legal_entity_foreign_descr,
            legal_entity_foreign_posta,
            legal_entity_foreign_provi,
            -- Place of Performance
            place_of_performance_code,
            place_of_performance_scope,
            pop_country_code                        AS "place_of_perform_country_c",
            pop_country_name                        AS "place_of_perform_country_n",
            pop_state_code                          AS "place_of_perfor_state_code",
            pop_state_name                          AS "place_of_perform_state_nam",
            pop_county_code                         AS "place_of_perform_county_co",
            pop_county_name                         AS "place_of_perform_county_na",
            pop_congressional_code                  AS "place_of_performance_congr",
            pop_zip5                                AS "place_of_performance_zip5",
            place_of_performance_zip4a,
            place_of_perform_zip_last4,
            pop_city_name                           AS "place_of_performance_city",
            place_of_performance_forei,
            -- Officer Amounts
            officer_1_name,
            officer_1_amount,
            officer_2_name,
            officer_2_amount,
            officer_3_name,
            officer_3_amount,
            officer_4_name,
            officer_4_amount,
            officer_5_name,
            officer_5_amount,
            -- Exclusively FABS
            afa_generated_unique,
            business_funds_ind_desc,
            business_funds_indicator,
            business_types,
            business_types_desc,
            cfda_number,
            cfda_title,
            correction_delete_indicatr,
            correction_delete_ind_desc,
            fain,
            funding_opportunity_goals,
            funding_opportunity_number,
            record_type,
            record_type_description,
            sai_number,
            uri
        FROM
            rpt.transaction_search
        WHERE
            is_fpds = False;
"""
