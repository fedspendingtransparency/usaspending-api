from django.db import models


class TransactionNormalizedChangeTracker(models.Model):
    """
    The purpose of this table is to track changes in the transaction_normalized
    table (inserts, updates, deletes).  This is how the universal_transaction
    table knows which transactions to insert, update, or delete.  This table
    only tracks that something changed, not the exact change.
    """
    transaction_id = models.BigIntegerField(primary_key=True)

    class Meta:
        db_table = "transaction_normalized_change_tracker"
