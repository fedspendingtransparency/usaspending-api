from django.db import models

from usaspending_api.accounts.models.appropriation_account_balances import AbstractAppropriationAccountBalances


class AppropriationAccountBalancesDownloadView(AbstractAppropriationAccountBalances):
    """
    Model based on a View to support File A downloads. Inherits the File A table's model to ensure that all
    necessary fields are in place to support previous download functionality while also adding additional fields
    that are either:
        * not easily queried through the Django ORM
        * need to be manually defined in the query for performance
    """

    # Overriding attributes from the Abstract Fields;
    # This needs to occur primarily for the values of "on_delete" and "related_name"
    submission = models.ForeignKey("submissions.SubmissionAttributes", models.DO_NOTHING)
    treasury_account_identifier = models.ForeignKey(
        "accounts.TreasuryAppropriationAccount",
        models.DO_NOTHING,
        db_column="treasury_account_identifier",
    )

    # Additional values from the View
    agency_identifier_name = models.TextField()
    allocation_transfer_agency_identifier_name = models.TextField()

    class Meta:
        db_table = "vw_appropriation_account_balances_download"
        managed = False
