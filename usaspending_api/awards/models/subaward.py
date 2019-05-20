from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.search import SearchVectorField
from django.db import models

from usaspending_api.common.models import DataSourceTrackedModel


class Subaward(DataSourceTrackedModel):

    id = models.AutoField(primary_key=True)

    award = models.ForeignKey("awards.Award", models.CASCADE, related_name="subawards", null=True)
    cfda = models.ForeignKey(
        "references.Cfda", models.DO_NOTHING, related_name="related_subawards", null=True
    )
    awarding_agency = models.ForeignKey(
        "references.Agency", models.DO_NOTHING, related_name="awarding_subawards", null=True
    )
    funding_agency = models.ForeignKey(
        "references.Agency", models.DO_NOTHING, related_name="funding_subawards", null=True
    )

    subaward_number = models.TextField(db_index=True)
    amount = models.DecimalField(max_digits=23, decimal_places=2)
    description = models.TextField(null=True, blank=True)

    recovery_model_question1 = models.TextField(null=True, blank=True)
    recovery_model_question2 = models.TextField(null=True, blank=True)

    action_date = models.DateField(blank=True, null=True)
    award_report_fy_month = models.IntegerField()
    award_report_fy_year = models.IntegerField()

    broker_award_id = models.IntegerField(
        blank=False,
        null=False,
        default=0,
        verbose_name="FSRS Award ID in the " "Broker",
        help_text="The ID of the parent award in broker",
        db_index=True,
    )
    internal_id = models.TextField(
        blank=False,
        null=False,
        default="",
        verbose_name="Internal ID of the parent Award",
        help_text="The internal of the parent award in broker from FSRS",
        db_index=True,
    )
    award_type = models.TextField(
        blank=False,
        null=False,
        default="unknown",
        verbose_name="Award Type",
        help_text="Whether the parent Award is a Procurement or a Grant",
        db_index=True,
    )

    keyword_ts_vector = SearchVectorField(null=True, blank=True)
    award_ts_vector = SearchVectorField(null=True, blank=True)
    recipient_name_ts_vector = SearchVectorField(null=True, blank=True)
    data_source = models.TextField(null=True, blank=True)

    latest_transaction_id = models.IntegerField(null=True, blank=True)
    last_modified_date = models.DateField(null=True, blank=True)
    total_obl_bin = models.TextField(null=True, blank=True)

    awarding_toptier_agency_name = models.TextField(null=True, blank=True)
    awarding_subtier_agency_name = models.TextField(null=True, blank=True)
    funding_toptier_agency_name = models.TextField(null=True, blank=True)
    funding_subtier_agency_name = models.TextField(null=True, blank=True)
    awarding_toptier_agency_abbreviation = models.TextField(null=True, blank=True)
    funding_toptier_agency_abbreviation = models.TextField(null=True, blank=True)
    awarding_subtier_agency_abbreviation = models.TextField(null=True, blank=True)
    funding_subtier_agency_abbreviation = models.TextField(null=True, blank=True)

    prime_award_type = models.TextField(null=True, blank=True)

    piid = models.TextField(null=True, blank=True)
    fain = models.TextField(null=True, blank=True)

    recipient_unique_id = models.TextField(null=True, blank=True)
    recipient_name = models.TextField(null=True, blank=True)
    dba_name = models.TextField(null=True, blank=True)
    parent_recipient_unique_id = models.TextField(null=True, blank=True)
    parent_recipient_name = models.TextField(null=True, blank=True)
    business_type_code = models.TextField(null=True, blank=True)
    business_type_description = models.TextField(null=True, blank=True)

    prime_recipient = models.ForeignKey("references.LegalEntity", models.DO_NOTHING, null=True)
    business_categories = ArrayField(models.TextField(), default=list)
    prime_recipient_name = models.TextField(null=True, blank=True)

    pulled_from = models.TextField(null=True, blank=True)
    type_of_contract_pricing = models.TextField(null=True, blank=True)
    type_set_aside = models.TextField(null=True, blank=True)
    extent_competed = models.TextField(null=True, blank=True)
    product_or_service_code = models.TextField(null=True, blank=True)
    product_or_service_description = models.TextField(null=True, blank=True)
    cfda_number = models.TextField(null=True, blank=True)
    cfda_title = models.TextField(null=True, blank=True)

    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.TextField(null=True, blank=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.TextField(null=True, blank=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.TextField(null=True, blank=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.TextField(null=True, blank=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.TextField(null=True, blank=True)

    recipient_location_country_code = models.TextField(null=True, blank=True)
    recipient_location_country_name = models.TextField(null=True, blank=True)
    recipient_location_state_code = models.TextField(null=True, blank=True)
    recipient_location_state_name = models.TextField(null=True, blank=True)
    recipient_location_county_code = models.TextField(null=True, blank=True)
    recipient_location_county_name = models.TextField(null=True, blank=True)
    recipient_location_city_code = models.TextField(null=True, blank=True)
    recipient_location_city_name = models.TextField(null=True, blank=True)
    recipient_location_zip4 = models.TextField(null=True, blank=True)
    recipient_location_zip5 = models.TextField(null=True, blank=True)
    recipient_location_street_address = models.TextField(null=True, blank=True)
    recipient_location_congressional_code = models.TextField(null=True, blank=True)
    recipient_location_foreign_postal_code = models.TextField(null=True, blank=True)

    pop_country_code = models.TextField(null=True, blank=True)
    pop_country_name = models.TextField(null=True, blank=True)
    pop_state_code = models.TextField(null=True, blank=True)
    pop_state_name = models.TextField(null=True, blank=True)
    pop_county_code = models.TextField(null=True, blank=True)
    pop_county_name = models.TextField(null=True, blank=True)
    pop_city_code = models.TextField(null=True, blank=True)
    pop_city_name = models.TextField(null=True, blank=True)
    pop_zip4 = models.TextField(null=True, blank=True)
    pop_street_address = models.TextField(null=True, blank=True)
    pop_congressional_code = models.TextField(null=True, blank=True)

    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "subaward"
