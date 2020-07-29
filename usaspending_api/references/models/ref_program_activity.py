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

        # There is a limitation in nearly all SQL databases where NULLs are not considered part of
        # unique constraints.  This is by design and is part of the spec.  This means it's possible
        # to insert duplicate rows if any of the columns in those rows contains a null.  To work
        # around this, either all of the columns need to be non-nullable or we need to coalesce
        # nullable columns.  We're going with the second option since this is an established table
        # and working through all the code to ensure nothing breaks because we're suddenly returning
        # blanks seems riskier.
        #
        # What does this mean?  Well, it means there's a unique constraint defined in SQL in one of
        # the migrations for this table.
