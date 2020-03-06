from django.db import models


class SummaryTransactionFedAcctView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()

    federal_account_id = models.IntegerField()
    treasury_account_id = models.IntegerField()
    agency_identifier = models.TextField()
    main_account_code = models.TextField()
    account_title = models.TextField()
    federal_account_display = models.TextField()

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = "summary_transaction_fed_acct_view"
