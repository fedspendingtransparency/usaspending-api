# Third-party app imports
import pytest
from model_mommy import mommy

# Imports from your apps
from usaspending_api.awards.models import Award, TransactionNormalized, TransactionFABS
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs
from usaspending_api.references.models import LegalEntity, Location


@pytest.mark.django_db()
def test_delete_fabs_success():
    """ Testing delete fabs works properly """

    # Award/Transaction/LE/Location deleted based on 1-1 transaction
    mommy.make(Award, id=1, latest_transaction_id=1, recipient_id=1, place_of_performance_id=1)
    mommy.make(TransactionNormalized, id=1, award_id=1, recipient_id=1, place_of_performance_id=1)
    mommy.make(TransactionFABS, transaction_id=1, afa_generated_unique='A')
    mommy.make(LegalEntity, legal_entity_id=1, location_id=2)
    mommy.make(Location, location_id=1)
    mommy.make(Location, location_id=2)

    # Award/Locations/LEs kept despite having one of their associated transactions removed
    mommy.make(Award, id=2, latest_transaction_id=2, recipient_id=3, place_of_performance_id=3)
    mommy.make(TransactionNormalized, id=2, award_id=2, recipient_id=2, place_of_performance_id=3,
               action_date='2019-01-01')
    mommy.make(TransactionNormalized, id=3, award_id=2, recipient_id=3, place_of_performance_id=4,
               action_date='2019-01-02')
    mommy.make(TransactionFABS, transaction_id=2, afa_generated_unique='B')
    mommy.make(TransactionFABS, transaction_id=3, afa_generated_unique='C')
    mommy.make(LegalEntity, legal_entity_id=2, location_id=5)
    mommy.make(LegalEntity, legal_entity_id=3, location_id=6)
    mommy.make(Location, location_id=3)
    mommy.make(Location, location_id=4)
    mommy.make(Location, location_id=5)
    mommy.make(Location, location_id=6)

    # Award/Transaction/LE/Locations untouched at all for control
    mommy.make(Award, id=3, latest_transaction_id=4, recipient_id=4, place_of_performance_id=7)
    mommy.make(TransactionNormalized, id=4, award_id=3, recipient_id=4, place_of_performance_id=7)
    mommy.make(TransactionFABS, transaction_id=4, afa_generated_unique='D')
    mommy.make(LegalEntity, legal_entity_id=4, location_id=8)
    mommy.make(Location, location_id=7)
    mommy.make(Location, location_id=8)

    # Main call
    updated_awards = delete_stale_fabs(['A', 'B'])
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

    recipient_ids = set([award.recipient_id for award in awards_left])
    expected_recipient_ids = {3, 4}
    assert recipient_ids == expected_recipient_ids

    ppop_ids = set([award.place_of_performance_id for award in awards_left])
    expected_ppop_ids = {4, 7}
    assert ppop_ids == expected_ppop_ids

    # Transaction Normalized
    transactions_left = TransactionNormalized.objects.all()

    transaction_norm_ids_left = set([transaction.id for transaction in transactions_left])
    expected_transaction_norm_ids_left = {3, 4}
    assert transaction_norm_ids_left == expected_transaction_norm_ids_left

    # Transaction FABS
    transactions_fabs_left = TransactionFABS.objects.all()

    transaction_fabs_afas_left = set([transaction_fabs.afa_generated_unique for transaction_fabs in
                                      transactions_fabs_left])
    expected_transaction_fabs_afas_left = {'C', 'D'}
    assert expected_transaction_fabs_afas_left == transaction_fabs_afas_left
