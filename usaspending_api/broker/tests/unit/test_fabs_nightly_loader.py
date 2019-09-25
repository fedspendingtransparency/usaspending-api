import pytest

from datetime import date
from model_mommy import mommy
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    Subaward,
    TransactionDelta,
    TransactionFABS,
    TransactionNormalized,
)
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs


@pytest.mark.django_db
def test_delete_stale_fabs():
    # Create awards and two transactions for each.  Also, create a record for each object affected
    # by delete_stale_fabs so we can monitor the effect our deletions have on them.
    for aid in range(1, 5):

        a = mommy.make("awards.Award", id=aid)
        mommy.make("awards.Subaward", award=a)
        mommy.make("awards.FinancialAccountsByAwards", award=a)

        # 100, 101, ... 400, 401
        for tid in range(aid * 100, aid * 100 + 2):

            t = mommy.make("awards.TransactionNormalized", id=tid, award=a, action_date=date(2000 + aid, 1, 2))
            mommy.make("awards.TransactionFABS", transaction=t, afa_generated_unique="AFA_{}".format(str(8000 + tid)))
            mommy.make("awards.TransactionDelta", transaction=t)
            a.latest_transaction = t
            a.save()

    # Our baseline.
    assert Award.objects.all().count() == 4
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 4
    assert FinancialAccountsByAwards.objects.all().count() == 4
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 4
    assert Subaward.objects.filter(award__isnull=False).count() == 4
    assert TransactionDelta.objects.all().count() == 8
    assert TransactionFABS.objects.all().count() == 8
    assert TransactionNormalized.objects.all().count() == 8

    # Nothing should change since these ids are invalid.
    delete_stale_fabs(["AFA_-1", "AFA_-2"])
    assert Award.objects.all().count() == 4
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 4
    assert FinancialAccountsByAwards.objects.all().count() == 4
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 4
    assert Subaward.objects.filter(award__isnull=False).count() == 4
    assert TransactionDelta.objects.all().count() == 8
    assert TransactionFABS.objects.all().count() == 8
    assert TransactionNormalized.objects.all().count() == 8

    # We're deleting a single transaction, so one transaction should be deleted.
    delete_stale_fabs(["AFA_8100"])
    assert Award.objects.all().count() == 4
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 4
    assert FinancialAccountsByAwards.objects.all().count() == 4
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 4
    assert Subaward.objects.filter(award__isnull=False).count() == 4
    assert TransactionDelta.objects.all().count() == 7
    assert TransactionFABS.objects.all().count() == 7
    assert TransactionNormalized.objects.all().count() == 7

    # We're deleting the other transaction for the same award as above so the award should go away.
    delete_stale_fabs(["AFA_8101"])
    assert Award.objects.all().count() == 3
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 3
    assert FinancialAccountsByAwards.objects.all().count() == 4
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 3
    assert Subaward.objects.all().count() == 4
    assert Subaward.objects.filter(award__isnull=False).count() == 3
    assert TransactionDelta.objects.all().count() == 6
    assert TransactionFABS.objects.all().count() == 6
    assert TransactionNormalized.objects.all().count() == 6

    # Just for giggles, delete a couple more transactions.
    delete_stale_fabs(["AFA_8200", "AFA_8201"])
    assert Award.objects.all().count() == 2
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 2
    assert FinancialAccountsByAwards.objects.all().count() == 4
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 2
    assert Subaward.objects.all().count() == 4
    assert Subaward.objects.filter(award__isnull=False).count() == 2
    assert TransactionDelta.objects.all().count() == 4
    assert TransactionFABS.objects.all().count() == 4
    assert TransactionNormalized.objects.all().count() == 4
