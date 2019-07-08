from django.db import models


class RefProgramActivity(models.Model):
    id = models.AutoField(primary_key=True)
    program_activity_code = models.TextField()
    program_activity_name = models.TextField(blank=True, null=True)
    budget_year = models.TextField(blank=True, null=True)
    responsible_agency_id = models.TextField(blank=True, null=True)
    allocation_transfer_agency_id = models.TextField(blank=True, null=True)
    main_account_code = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "ref_program_activity"
        unique_together = (
            "program_activity_code",
            "program_activity_name",
            "responsible_agency_id",
            "allocation_transfer_agency_id",
            "main_account_code",
            "budget_year",
        )
