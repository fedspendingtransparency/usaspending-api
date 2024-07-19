from django.db import models


class CovidFABASpending(models.Model):
    id = models.BigAutoField(primary_key=True)
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
