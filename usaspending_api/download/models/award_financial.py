from django.db import models

from usaspending_api.awards.models import AbstractFinancialAccountsByAwards


class AwardFinancialView(AbstractFinancialAccountsByAwards):
    """
    Model based on a View to support File A downloads. Inherits the File A table's model to ensure that all
    necessary fields are in place to support previous download functionality while also adding additional fields
    that are either:
        * not easily queried through the Django ORM
        * need to be manually defined in the query for performance
    """

    agency_identifier_name = models.TextField()
    allocation_transfer_agency_identifier_name = models.TextField()

    class Meta:
        db_table = "vw_award_financial"
        managed = False
