import pytest

from model_mommy import mommy
from usaspending_api.awards.models import Award, TransactionNormalized, TransactionFABS
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs
from usaspending_api.broker.helpers.upsert_fabs_transactions import upsert_fabs_transactions
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.transactions.models import SourceAssistanceTransaction


@pytest.mark.django_db(transaction=True)
def test_delete_fabs_success():
    """ Testing delete fabs works properly """

    # Award/Transaction deleted based on 1-1 transaction
    mommy.make(Award, id=1, generated_unique_award_id="TEST_AWARD_1")
    mommy.make(TransactionNormalized, id=1, award_id=1, unique_award_key="TEST_AWARD_1")
    mommy.make(
        TransactionFABS, transaction_id=1, published_fabs_id=301, unique_award_key="TEST_AWARD_1"
    )

    # Award kept despite having one of their associated transactions removed
    mommy.make(Award, id=2, generated_unique_award_id="TEST_AWARD_2")
    mommy.make(TransactionNormalized, id=2, award_id=2, action_date="2019-01-01", unique_award_key="TEST_AWARD_2")
    mommy.make(TransactionNormalized, id=3, award_id=2, action_date="2019-01-02", unique_award_key="TEST_AWARD_2")
    mommy.make(
        TransactionFABS, transaction_id=2, published_fabs_assistance_id=302, unique_award_key="TEST_AWARD_2"
    )
    mommy.make(
        TransactionFABS, transaction_id=3, published_fabs_id=303, unique_award_key="TEST_AWARD_2"
    )

    # Award/Transaction untouched at all as control
    mommy.make(Award, id=3, generated_unique_award_id="TEST_AWARD_3")
    mommy.make(TransactionNormalized, id=4, award_id=3, unique_award_key="TEST_AWARD_3")
    mommy.make(
        TransactionFABS, transaction_id=4, published_fabs_id=304, unique_award_key="TEST_AWARD_3"
    )

    # Award is not deleted; old transaction deleted; new transaction uses old award
    mommy.make(Award, id=4, generated_unique_award_id="TEST_AWARD_4")
    mommy.make(TransactionNormalized, id=5, award_id=4, unique_award_key="TEST_AWARD_4")
    mommy.make(
        TransactionFABS, transaction_id=5, published_fabs_id=305, unique_award_key="TEST_AWARD_4"
    )
    mommy.make(
        SourceAssistanceTransaction,
        published_fabs_id=306,
        afa_generated_unique="TEST_TRANSACTION_6",
        unique_award_key="TEST_AWARD_4",
        is_active=True,
        modified_at="2022-02-18 18:27:50.813471",
        created_at="2022-02-18 18:27:50.813471",
        updated_at="2022-02-18 18:27:50.813471",
        action_date="2022-02-18 18:27:50.813471",
    )

    update_awards()

    # Main call
    updated_and_delete_awards = delete_stale_fabs([301, 302, 305])
    expected_updated_and_delete_awards = [1, 2, 4]
    assert sorted(updated_and_delete_awards) == expected_updated_and_delete_awards

    # Update and Delete Awards
    upsert_fabs_transactions(ids_to_upsert=[306], update_and_delete_award_ids=updated_and_delete_awards)

    # Awards
    awards_left = Award.objects.all()
    award_ids_left = set([award.id for award in awards_left])
    expected_awards_ids_left = [2, 3, 4]
    assert sorted(award_ids_left) == expected_awards_ids_left
    assert len(award_ids_left) == len(expected_awards_ids_left)

    latest_transaction_ids = set([award.latest_transaction_id for award in awards_left])
    new_award_transaction_id = TransactionNormalized.objects.filter(award_id=4).values_list("id", flat=True).first()
    expected_latest_transaction_ids = sorted([3, 4, new_award_transaction_id])
    assert sorted(latest_transaction_ids) == expected_latest_transaction_ids

    # Transaction Normalized
    transactions_left = TransactionNormalized.objects.all()

    transaction_norm_ids_left = set([transaction.id for transaction in transactions_left])
    expected_transaction_norm_ids_left = sorted([3, 4, new_award_transaction_id])
    assert sorted(transaction_norm_ids_left) == expected_transaction_norm_ids_left

    # Transaction FABS
    transactions_fabs_left = TransactionFABS.objects.all()

    transaction_fabs_left = set(
        [transaction_fabs.published_fabs_id for transaction_fabs in transactions_fabs_left]
    )
    expected_transaction_fabs_left = [303, 304, 306]
    assert sorted(transaction_fabs_left) == expected_transaction_fabs_left
