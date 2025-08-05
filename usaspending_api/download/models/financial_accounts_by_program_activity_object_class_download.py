from django.db import models

from usaspending_api.financial_activities.models import AbstractFinancialAccountsByProgramActivityObjectClass


class FinancialAccountsByProgramActivityObjectClassDownloadView(AbstractFinancialAccountsByProgramActivityObjectClass):
    """
    Model based on a View to support File B downloads. Inherits the File B table's model to ensure that all
    necessary fields are in place to support previous download functionality while also adding additional fields
    that are either:
        * not easily queried through the Django ORM
        * need to be manually defined in the query for performance
    """

    # Overriding attributes from the Abstract Fields;
    # This needs to occur primarily for the values of "on_delete" and "related_name"
    submission = models.ForeignKey("submissions.SubmissionAttributes", models.DO_NOTHING)
    treasury_account = models.ForeignKey("accounts.TreasuryAppropriationAccount", models.DO_NOTHING, null=True)

    # Additional values from the View
    agency_identifier_name = models.TextField()
    allocation_transfer_agency_identifier_name = models.TextField()

    class Meta:
        db_table = "vw_financial_accounts_by_program_activity_object_class_download"
        managed = False
