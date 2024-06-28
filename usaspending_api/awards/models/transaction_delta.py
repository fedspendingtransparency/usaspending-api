"""
This model was originally created to satisfy DEV-2417:

    As a data re-purposer, I need monthly delta files to also include transactions which
    were added/modified by appropriate data change operation scripts so that the delta
    files keep me in sync with USAspending.gov data.

TransactionDelta (transaction_delta) is used to track transaction records that have been
created/updated outside of the nightly pipeline.  These are most likely to be one-time
data updates/corrections which would otherwise not be captured by the monthly delta process.

To use this table, simply record the transaction_normalized.id of the offending transaction
and the current timestamp (created_at).  If said transaction already exists, update it with
a fresher timestamp.

The monthly delta process will handle the rest.
"""

from datetime import datetime, timezone
from django.db import models, transaction
from django.db.utils import IntegrityError
from usaspending_api.awards.models.transaction_normalized import TransactionNormalized


# To keep queries from getting too large.
CHUNK_SIZE = 5000


class TransactionDeltaManager(models.Manager):
    def delete_by_created_at(self, max_created_at):
        delete_count, _ = self.get_queryset().filter(created_at__lte=max_created_at).delete()
        return delete_count

    def get_max_created_at(self):
        return self.get_queryset().aggregate(models.Max("created_at"))["created_at__max"]

    def update_or_create_transaction(self, transaction_id):
        """
        Update or create the specified transaction id.
        """
        if transaction_id:
            self.update_or_create_transactions([transaction_id])

    @staticmethod
    def update_or_create_transactions(transaction_ids):
        """
        Update or create the specified transaction ids.
        """
        if transaction_ids:
            # Duplicates will cause us problems so let's eliminate them.
            transaction_ids = list(set(transaction_ids))

            # TransactionCheck
            err_transactions = TransactionNormalized.objects.filter(id__in=transaction_ids).count()
            if err_transactions != len(transaction_ids):
                raise IntegrityError("transaction_ids not found in transaction_normalized")

            created_at = datetime.now(timezone.utc)
            with transaction.atomic():

                # Perform upserts in chunks.
                for chunk_start in range(0, len(transaction_ids), CHUNK_SIZE):
                    chunk_of_ids = transaction_ids[chunk_start : chunk_start + CHUNK_SIZE]
                    chunk_of_inserts = tuple(
                        TransactionDelta(transaction_id=transaction_id, created_at=created_at)
                        for transaction_id in chunk_of_ids
                    )

                    # Fake an upsert by first deleting transactions then inserting.
                    TransactionDelta.objects.filter(pk__in=chunk_of_ids).delete()
                    TransactionDelta.objects.bulk_create(chunk_of_inserts)


class TransactionDelta(models.Model):

    transaction = models.OneToOneField(
        "awards.TransactionNormalized", on_delete=models.CASCADE, primary_key=True, db_constraint=False
    )
    created_at = models.DateTimeField()

    objects = TransactionDeltaManager()

    class Meta:
        db_table = "transaction_delta"
