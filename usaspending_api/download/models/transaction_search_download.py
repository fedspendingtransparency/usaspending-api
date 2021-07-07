from django.db import models

from usaspending_api.search.models import AbstractTransactionSearch


class TransactionSearchDownloadView(AbstractTransactionSearch):
    """
    Model based on a View to support Transaction downloads. Inherits the TransactionSearch table's model to ensure that
    all necessary fields are in place to support previous download functionality while also adding additional fields
    that are either:
        * not easily queried through the Django ORM
        * need to be manually defined in the query for performance
    """

    # Additional values from the View
    treasury_accounts_funding_this_award = models.TextField()
    federal_accounts_funding_this_award = models.TextField()
    object_classes_funding_this_award = models.TextField()
    program_activities_funding_this_award = models.TextField()

    class Meta:
        db_table = "vw_transaction_search_download"
        managed = False
