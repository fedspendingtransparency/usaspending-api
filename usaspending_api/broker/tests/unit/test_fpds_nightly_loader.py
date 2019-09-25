import pytest

from datetime import date
from model_mommy import mommy
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    ParentAward,
    Subaward,
    TransactionDelta,
    TransactionFPDS,
    TransactionNormalized,
)
from usaspending_api.broker.management.commands.fpds_nightly_loader import AWARD_UPDATE_ID_LIST, Command


@pytest.mark.django_db
def test_delete_stale_fpds():

    # Create parent and child awards and two transactions for each.  Also, create a record for each
    # object affected by delete_stale_fpds so we can monitor the effect our deletions have on them.
    for aid in range(1, 5):

        pa = mommy.make("awards.Award", id=aid + 1000)
        ppa = mommy.make("awards.ParentAward", award=pa)
        a = mommy.make("awards.Award", id=aid)
        mommy.make("awards.ParentAward", award=a, parent_award=ppa)
        mommy.make("awards.Subaward", award=pa)
        mommy.make("awards.Subaward", award=a)
        mommy.make("awards.FinancialAccountsByAwards", award=pa)
        mommy.make("awards.FinancialAccountsByAwards", award=a)

        # 100, 101, ... 400, 401
        for tid in range(aid * 100, aid * 100 + 2):

            t = mommy.make("awards.TransactionNormalized", id=tid, award=a, action_date=date(2000 + aid, 1, 2))
            mommy.make("awards.TransactionFPDS", transaction=t, detached_award_procurement_id=8000 + tid)
            mommy.make("awards.TransactionDelta", transaction=t)
            a.latest_transaction = t
            a.save()

            t = mommy.make("awards.TransactionNormalized", id=tid + 2000, award=pa, action_date=date(2000 + aid, 1, 3))
            mommy.make("awards.TransactionFPDS", transaction=t, detached_award_procurement_id=9000 + tid)
            mommy.make("awards.TransactionDelta", transaction=t)
            pa.latest_transaction = t
            pa.save()

    # Our baseline.
    assert Award.objects.all().count() == 8
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 8
    assert FinancialAccountsByAwards.objects.all().count() == 8
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 8
    assert ParentAward.objects.all().count() == 8
    assert ParentAward.objects.filter(parent_award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 8
    assert Subaward.objects.filter(award__isnull=False).count() == 8
    assert TransactionDelta.objects.all().count() == 16
    assert TransactionFPDS.objects.all().count() == 16
    assert TransactionNormalized.objects.all().count() == 16
    assert AWARD_UPDATE_ID_LIST == []

    # Nothing should change since these ids are invalid.
    Command.delete_stale_fpds([-1, -2])
    assert Award.objects.all().count() == 8
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 8
    assert FinancialAccountsByAwards.objects.all().count() == 8
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 8
    assert ParentAward.objects.all().count() == 8
    assert ParentAward.objects.filter(parent_award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 8
    assert Subaward.objects.filter(award__isnull=False).count() == 8
    assert TransactionDelta.objects.all().count() == 16
    assert TransactionFPDS.objects.all().count() == 16
    assert TransactionNormalized.objects.all().count() == 16
    assert AWARD_UPDATE_ID_LIST == []

    # We're deleting a single transaction, so one transaction should be deleted and one award added to update list.
    Command.delete_stale_fpds([8100])
    assert Award.objects.all().count() == 8
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 7
    assert FinancialAccountsByAwards.objects.all().count() == 8
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 8
    assert ParentAward.objects.all().count() == 8
    assert ParentAward.objects.filter(parent_award__isnull=False).count() == 4
    assert Subaward.objects.all().count() == 8
    assert Subaward.objects.filter(award__isnull=False).count() == 8
    assert TransactionDelta.objects.all().count() == 15
    assert TransactionFPDS.objects.all().count() == 15
    assert TransactionNormalized.objects.all().count() == 15
    assert AWARD_UPDATE_ID_LIST == [1]

    # We're deleting the other transaction for the same award as above so the award should go away.
    Command.delete_stale_fpds([8101])
    assert Award.objects.all().count() == 7
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 7
    assert FinancialAccountsByAwards.objects.all().count() == 8
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 7
    assert ParentAward.objects.all().count() == 7
    assert ParentAward.objects.filter(parent_award__isnull=False).count() == 3
    assert Subaward.objects.all().count() == 8
    assert Subaward.objects.filter(award__isnull=False).count() == 7
    assert TransactionDelta.objects.all().count() == 14
    assert TransactionFPDS.objects.all().count() == 14
    assert TransactionNormalized.objects.all().count() == 14
    assert AWARD_UPDATE_ID_LIST == [1]  # side effects are bad

    # Delete all the transactions for a parent award.
    Command.delete_stale_fpds([9200, 9201])
    assert Award.objects.all().count() == 6
    assert Award.objects.filter(latest_transaction__isnull=False).count() == 6
    assert FinancialAccountsByAwards.objects.all().count() == 8
    assert FinancialAccountsByAwards.objects.filter(award__isnull=False).count() == 6
    assert ParentAward.objects.all().count() == 6
    assert ParentAward.objects.filter(parent_award__isnull=False).count() == 2
    assert Subaward.objects.all().count() == 8
    assert Subaward.objects.filter(award__isnull=False).count() == 6
    assert TransactionDelta.objects.all().count() == 12
    assert TransactionFPDS.objects.all().count() == 12
    assert TransactionNormalized.objects.all().count() == 12
    assert AWARD_UPDATE_ID_LIST == [1, 2]
