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

    agency_identifier_name = models.TextField()
    allocation_transfer_agency_identifier_name = models.TextField()

    class Meta:
        db_table = "vw_financial_accounts_by_program_activity_object_class_download"
        managed = False
