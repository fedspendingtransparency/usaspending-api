import datetime

from model_mommy import mommy
import pytest

from usaspending_api.etl.award_helpers import update_awards, update_contract_awards
from usaspending_api.references.models import Agency


@pytest.mark.django_db
def test_award_update_from_latest_transaction(agencies):
    """Test awards fields that should be updated with most recent transaction info."""

    agency1 = Agency.objects.get(id=1)
    agency2 = Agency.objects.get(id=2)

    award = mommy.make(
        'awards.Award',
        awarding_agency=agency1,
        period_of_performance_current_end_date=datetime.date(2016, 1, 1),
        description='original award'
    )

    # adding transaction with same info should not change award values
    mommy.make(
        'awards.Transaction',
        award=award,
        awarding_agency=award.awarding_agency,
        period_of_performance_current_end_date=award.period_of_performance_current_end_date,
        description=award.description,
        action_date=datetime.date(2016, 2, 1)
    )

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency1
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == 'original award'

    # adding an older transaction with different info updates award's total
    # obligation amt but doesn't affect the other values
    mommy.make(
        'awards.Transaction',
        award=award,
        awarding_agency=agency2,
        period_of_performance_current_end_date=datetime.date(2017, 1, 1),
        description='new description',
        action_date=datetime.date(2016, 1, 1)
    )
    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency1
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == 'original award'

    # adding an newer transaction with different info updates award's total
    # obligation amt and also overrides other values
    mommy.make(
        'awards.Transaction',
        id=999,
        award=award,
        awarding_agency=agency2,
        period_of_performance_current_end_date=datetime.date(2010, 1, 1),
        description='new description',
        action_date=datetime.date(2017, 1, 1)
    )

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency2
    assert award.period_of_performance_current_end_date == datetime.date(2010, 1, 1)
    assert award.description == 'new description'


@pytest.mark.django_db
def test_award_update_from_earliest_transaction():
    """Test awards fields that should be updated with most earliest transaction info."""

    award = mommy.make('awards.Award')
    mommy.make(
        'awards.Transaction',
        award=award,
        # since this is the award's first transaction,
        # the txn action_date will become the award
        # signed date
        action_date=datetime.date(2016, 1, 1)
    )

    # adding later transaction should not change award values
    mommy.make(
        'awards.Transaction',
        award=award,
        action_date=datetime.date(2017, 1, 1)
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2016, 1, 1)

    # adding earlier transaction should update award values
    mommy.make(
        'awards.Transaction',
        award=award,
        action_date=datetime.date(2010, 1, 1)
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2010, 1, 1)


@pytest.mark.django_db
def test_award_update_obligated_amt():
    """Test that the award obligated amt updates as child transactions change."""

    award = mommy.make('awards.Award', total_obligation=1000)
    mommy.make(
        'awards.Transaction',
        award=award,
        federal_action_obligation=1000,
        _quantity=5
    )

    update_awards()
    award.refresh_from_db()

    assert award.total_obligation == 5000


@pytest.mark.django_db
def test_award_update_from_contract_transaction():
    """Test award updates specific to contract transactions."""

    # for contract type transactions,
    # the potential_total_value_of_award field
    # should updte the corresponding field on the award table
    award = mommy.make('awards.Award')
    txn = mommy.make('awards.Transaction', award=award)
    mommy.make(
        'awards.TransactionContract',
        transaction=txn,
        potential_total_value_of_award=1000
    )

    update_contract_awards()
    award.refresh_from_db()

    assert award.potential_total_value_of_award == 1000


@pytest.mark.skip(reason="deletion feature not yet implemented")
@pytest.mark.django_db
def test_deleted_transactions():
    """Test that award values are updated correctly when a txn is deleted."""
    # writing these tests revealed that we're not updating awards fields
    # when transactions are deleted. since the Transaction model's delete()
    # method may not fire during a bulk deletion, we may want to use a signal
    # rather than override delete()
