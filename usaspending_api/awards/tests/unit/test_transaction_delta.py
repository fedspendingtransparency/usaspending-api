from django.db.utils import IntegrityError
from django.test import TransactionTestCase
from model_bakery import baker
from time import sleep
from usaspending_api.awards.models import TransactionDelta
from usaspending_api.awards.models.transaction_delta import CHUNK_SIZE


class TransactionDeltaTestCase(TransactionTestCase):
    def setUp(self):
        for _id in range(1, 5):
            baker.make("search.TransactionSearch", transaction_id=_id, is_fpds=True)
            baker.make("search.AwardSearch", award_id=_id, latest_transaction_id=_id)

    @staticmethod
    def test_get_max_created_at():
        assert TransactionDelta.objects.count() == 0

        TransactionDelta.objects.update_or_create_transaction(1)
        sleep(0.1)
        TransactionDelta.objects.update_or_create_transaction(2)
        max_created_at = TransactionDelta.objects.get(pk=2).created_at

        assert TransactionDelta.objects.get_max_created_at() == max_created_at

    def test_update_or_create_transaction(self):
        assert TransactionDelta.objects.count() == 0

        TransactionDelta.objects.update_or_create_transaction(1)
        assert TransactionDelta.objects.count() == 1
        assert TransactionDelta.objects.get(pk=1).created_at is not None

        TransactionDelta.objects.update_or_create_transaction(None)
        assert TransactionDelta.objects.count() == 1

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transaction("A")
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transaction(-1)

        # Get the created_at for transaction 1.  Sleep half a tick.
        # Update transaction 1.  Make sure new timestamp is newer.
        created_at = TransactionDelta.objects.get(pk=1).created_at
        sleep(0.1)
        TransactionDelta.objects.update_or_create_transaction(1)
        new_created_at = TransactionDelta.objects.get(pk=1).created_at
        assert new_created_at > created_at

    def test_update_or_create_transactions(self):
        assert TransactionDelta.objects.count() == 0

        TransactionDelta.objects.update_or_create_transactions([1])
        assert TransactionDelta.objects.count() == 1

        TransactionDelta.objects.update_or_create_transactions(None)
        assert TransactionDelta.objects.count() == 1

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transactions(["A"])
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([-1])

        TransactionDelta.objects.update_or_create_transactions([1, 2])
        assert TransactionDelta.objects.count() == 2

        with self.assertRaises(ValueError):
            TransactionDelta.objects.update_or_create_transactions([1, "A"])
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([1, -1])

        # Get the created_at for transaction 1.  Sleep half a tick.
        # Update transaction 1.  Make sure new timestamp is newer.
        created_at = TransactionDelta.objects.get(pk=1).created_at
        sleep(0.1)
        TransactionDelta.objects.update_or_create_transactions([1])
        new_created_at = TransactionDelta.objects.get(pk=1).created_at
        assert new_created_at > created_at

        # We only save unique transaction ids so there should be only three.
        TransactionDelta.objects.update_or_create_transactions([3] * (CHUNK_SIZE * 2))
        assert TransactionDelta.objects.count() == 3

        # None of these should be saved since one will fail.
        with self.assertRaises(IntegrityError):
            TransactionDelta.objects.update_or_create_transactions([4] * (CHUNK_SIZE * 2) + [-1])
        assert TransactionDelta.objects.filter(pk=4).first() is None
