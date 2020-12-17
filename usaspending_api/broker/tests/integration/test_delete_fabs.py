import pytest

from model_mommy import mommy
from usaspending_api.awards.models import Award, TransactionNormalized, TransactionFABS
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs


@pytest.mark.django_db()
def test_delete_fabs_success():
    """ Testing delete fabs works properly """

    # Award/Transaction deleted based on 1-1 transaction
    mommy.make(Award, id=1, latest_transaction_id=1, generated_unique_award_id="TEST_AWARD_1")
    mommy.make(TransactionNormalized, id=1, award_id=1, unique_award_key="TEST_AWARD_1")
    mommy.make(
        TransactionFABS, transaction_id=1, published_award_financial_assistance_id=301, unique_award_key="TEST_AWARD_1"
    )

    # Award kept despite having one of their associated transactions removed
    mommy.make(Award, id=2, latest_transaction_id=2, generated_unique_award_id="TEST_AWARD_2")
    mommy.make(TransactionNormalized, id=2, award_id=2, action_date="2019-01-01", unique_award_key="TEST_AWARD_2")
    mommy.make(TransactionNormalized, id=3, award_id=2, action_date="2019-01-02", unique_award_key="TEST_AWARD_2")
    mommy.make(
        TransactionFABS, transaction_id=2, published_award_financial_assistance_id=302, unique_award_key="TEST_AWARD_2"
    )
    mommy.make(
        TransactionFABS, transaction_id=3, published_award_financial_assistance_id=303, unique_award_key="TEST_AWARD_2"
    )

    # Award/Transaction untouched at all as control
    mommy.make(Award, id=3, latest_transaction_id=4, generated_unique_award_id="TEST_AWARD_3")
    mommy.make(TransactionNormalized, id=4, award_id=3, unique_award_key="TEST_AWARD_3")
    mommy.make(
        TransactionFABS, transaction_id=4, published_award_financial_assistance_id=304, unique_award_key="TEST_AWARD_3"
    )

    # Main call
    updated_awards = delete_stale_fabs([301, 302])
    expected_update_awards = [2]
    assert updated_awards == expected_update_awards

    # Awards
    awards_left = Award.objects.all()

    award_ids_left = set([award.id for award in awards_left])
    expected_awards_ids_left = {2, 3}
    assert award_ids_left == expected_awards_ids_left

    latest_transaction_ids = set([award.latest_transaction_id for award in awards_left])
    expected_latest_transaction_ids = {3, 4}
    assert latest_transaction_ids == expected_latest_transaction_ids

    # Transaction Normalized
    transactions_left = TransactionNormalized.objects.all()

    transaction_norm_ids_left = set([transaction.id for transaction in transactions_left])
    expected_transaction_norm_ids_left = {3, 4}
    assert transaction_norm_ids_left == expected_transaction_norm_ids_left

    # Transaction FABS
    transactions_fabs_left = TransactionFABS.objects.all()

    transaction_fabs_left = set(
        [transaction_fabs.published_award_financial_assistance_id for transaction_fabs in transactions_fabs_left]
    )
    expected_transaction_fabs_left = {303, 304}
    assert expected_transaction_fabs_left == transaction_fabs_left
