from django.db import models


# Reference tables
class RefObjectClassCode(models.Model):
    object_class = models.CharField(primary_key=True, max_length=4)
    max_object_class_name = models.CharField(max_length=60, blank=True, null=True)
    direct_or_reimbursable = models.CharField(max_length=25, blank=True, null=True)
    label = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_object_class_code'


class RefProgramActivity(models.Model):
    program_activity_code = models.CharField(max_length=4)
    program_activity_name = models.CharField(max_length=164)
    budget_year = models.CharField(max_length=4, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_program_activity'
        unique_together = (('program_activity_code', 'program_activity_name'),)
