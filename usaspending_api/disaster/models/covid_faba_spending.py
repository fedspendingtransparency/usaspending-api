from django.db import models


class CovidFABASpending(models.Model):
    id = models.AutoField(primary_key=True)

    spending_level = models.TextField()

    funding_toptier_agency_id = models.TextField(null=True)
    funding_toptier_agency_code = models.TextField(null=True)
    funding_toptier_agency_name = models.TextField(null=True)
    funding_subtier_agency_id = models.IntegerField(null=True)
    funding_subtier_agency_code = models.TextField(null=True)
    funding_subtier_agency_name = models.TextField(null=True)

    funding_federal_account_id = models.IntegerField(null=True)
    funding_federal_account_code = models.TextField(null=True)
    funding_federal_account_name = models.TextField(null=True)
    funding_treasury_account_id = models.IntegerField(null=True)
    funding_treasury_account_code = models.TextField(null=True)
    funding_treasury_account_name = models.TextField(null=True)

    funding_major_object_class_id = models.TextField(null=True)
    funding_major_object_class_code = models.TextField(null=True)
    funding_major_object_class_name = models.TextField(null=True)
    funding_object_class_id = models.IntegerField(null=True)
    funding_object_class_code = models.TextField(null=True)
    funding_object_class_name = models.TextField(null=True)

    defc = models.TextField()
    award_type = models.TextField(null=True)
    award_count = models.IntegerField()
    obligation_sum = models.DecimalField(null=True, max_digits=23, decimal_places=2)
    outlay_sum = models.DecimalField(null=True, max_digits=23, decimal_places=2)
    face_value_of_loan = models.DecimalField(null=True, max_digits=23, decimal_places=2)

    class Meta:
        managed = True
        db_table = '"rpt"."covid_faba_spending"'
        indexes = [
            # Helpful for the /disaster/award endpoints
            models.Index(name="spending_level_idx", fields=["spending_level"]),
            # These are the two columns that are required for filtering
            models.Index(name="spending_level_defc_idx", fields=["spending_level", "defc"]),
            # award_type is an optional filter value so if it's provided then this index is the best for performance
            models.Index(name="spending_defc_award_type_idx", fields=["spending_level", "defc", "award_type"]),
        ]
