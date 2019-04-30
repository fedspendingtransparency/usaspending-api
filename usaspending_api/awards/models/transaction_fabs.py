from django.core.exceptions import ObjectDoesNotExist
from django.db import models

from usaspending_api.references.models import Cfda


class TransactionFABS(models.Model):
    transaction = models.OneToOneField(
        "awards.TransactionNormalized",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="assistance_data",
    )
    published_award_financial_assistance_id = models.IntegerField(blank=True, null=True, db_index=True)
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
    cfda_number = models.TextField(blank=True, null=True, db_index=False)
    cfda_title = models.TextField(blank=True, null=True)
    correction_delete_indicatr = models.TextField(blank=True, null=True)
    correction_delete_ind_desc = models.TextField(blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    fain = models.TextField(blank=True, null=True, db_index=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    fiscal_year_and_quarter_co = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(null=False, default=False)
    is_historical = models.NullBooleanField()
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
    record_type = models.IntegerField(blank=True, null=True)
    record_type_description = models.TextField(blank=True, null=True)
    sai_number = models.TextField(blank=True, null=True)
    total_funding_amount = models.TextField(blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True, db_index=True)
    submission_id = models.IntegerField(blank=True, null=True)
    unique_award_key = models.TextField(null=True, db_index=True)  # From broker.

    # Timestamp field auto generated by broker
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True, db_index=True)

    @classmethod
    def get_or_create_2(cls, transaction, **kwargs):
        try:
            if not transaction.newer_than(kwargs):
                for (k, v) in kwargs.items():
                    setattr(transaction.assistance_data, k, v)
        except ObjectDoesNotExist:
            transaction.assistance_data = cls(**kwargs)
        return transaction.assistance_data

    @property
    def cfda_objectives(self):
        cfda = Cfda.objects.filter(program_number=self.cfda_number).first()
        if cfda:
            return cfda.objectives

    class Meta:
        db_table = "transaction_fabs"
        unique_together = (("awarding_sub_tier_agency_c", "award_modification_amendme", "fain", "uri"),)
