from django.db.utils import IntegrityError
from django.test import TransactionTestCase
from model_mommy import mommy
from time import sleep
from usaspending_api.awards.models import TransactionDelta
from usaspending_api.awards.models.transaction_delta import CHUNK_SIZE


class TransactionDeltaTestCase(TransactionTestCase):

    def setUp(self):
        for _id in range(1, 5):
            mommy.make('awards.TransactionNormalized', id=_id)
            mommy.make('awards.TransactionFPDS', transaction_id=_id)
            mommy.make('awards.Award', id=_id, latest_transaction_id=_id)

    def test_stuff(self):
        assert TransactionDelta.objects.count() == 0

        TransactionDelta.objects.update_or_create_transaction(1)
        assert TransactionDelta.objects.count() == 1
        assert TransactionDelta.objects.get(pk=1).created_at is not None

        TransactionDelta.objects.update_or_create_transaction(None)
        assert TransactionDelta.objects.count() == 1

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transaction('A')
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transaction(-1)

        TransactionDelta.objects.update_or_create_transactions([1])
        assert TransactionDelta.objects.count() == 1

        TransactionDelta.objects.update_or_create_transactions(None)
        assert TransactionDelta.objects.count() == 1

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transactions(['A'])
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([-1])

        TransactionDelta.objects.update_or_create_transactions([1, 2])
        assert TransactionDelta.objects.count() == 2

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transactions([1, 'A'])
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([1, -1])

        # Get the created_at for transaction 1.  Sleep half a tick.
        # Update transaction 1.  Make sure new timestamp is newer.
        created_at = TransactionDelta.objects.get(pk=1).created_at
        sleep(0.1)
        TransactionDelta.objects.update_or_create_transaction(1)
        new_created_at = TransactionDelta.objects.get(pk=1).created_at
        assert new_created_at > created_at

        TransactionDelta.objects.update_or_create_transactions([3] * (CHUNK_SIZE * 2))
        assert TransactionDelta.objects.count() == 3

        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([4] * (CHUNK_SIZE * 2) + [-1])

        # NONE of the previous transactions should have succeeded so there should be
        # no 4 in the database.
        assert TransactionDelta.objects.filter(pk=4).first() is None
