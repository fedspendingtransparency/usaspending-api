from decimal import Decimal

import pytest

from model_mommy import mommy

from usaspending_api.accounts.models import AppropriationAccountBalances, AppropriationAccountBalancesQuarterly
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
@pytest.mark.django_db
def tas_balances_data():
    sub1 = mommy.make("submissions.SubmissionAttributes")
    sub2 = mommy.make("submissions.SubmissionAttributes", previous_submission=sub1)
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount")

    mommy.make(
        "accounts.AppropriationAccountBalances",
        submission=sub1,
        treasury_account_identifier=tas1,
        budget_authority_unobligated_balance_brought_forward_fyb=None,
        budget_authority_appropriated_amount_cpe=10,
        unobligated_balance_cpe=99.99,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        submission=sub2,
        treasury_account_identifier=tas1,
        budget_authority_unobligated_balance_brought_forward_fyb=6.99,
        budget_authority_appropriated_amount_cpe=10,
        unobligated_balance_cpe=100,
    )


@pytest.mark.django_db
def test_get_quarterly_numbers(tas_balances_data):
    """Test the function that generates quarterly tas balances."""
    # retrieve all quarterly numbers and test results
    quarters = AppropriationAccountBalances.get_quarterly_numbers()
    # number of quarterly adjusted records should = number of records
    # in AppropriationAccountBalances
    assert len(list(quarters)) == 2

    # submission 1: has no previous subission
    # submission 2: its previous submission is submission 1
    sub1 = SubmissionAttributes.objects.get(previous_submission__isnull=True)
    sub2 = SubmissionAttributes.objects.get(previous_submission__isnull=False)

    for q in quarters:
        if q.submission == sub1:
            # qtrly values for year's first submission should remain unchanged
            assert q.budget_authority_appropriated_amount_cpe == Decimal("10.00")
            assert q.unobligated_balance_cpe == Decimal("99.99")
        else:
            # qtrly values for year's 2nd submission should be equal to 2nd
            # submission values - first submission values
            assert q.budget_authority_appropriated_amount_cpe == Decimal("0.00")
            assert q.unobligated_balance_cpe == Decimal(".01")

    # test getting quarterly results for a specific submission

    quarters = AppropriationAccountBalances.get_quarterly_numbers(sub2.submission_id)
    # number of quarterly adjusted records should = number of records
    # in AppropriationAccountBalances
    assert len(list(quarters)) == 1

    # requesting data for non-existent submission returns zero records
    quarters = AppropriationAccountBalances.get_quarterly_numbers(-888)
    assert len(list(quarters)) == 0


@pytest.mark.django_db
def test_get_quarterly_null_previous_submission(tas_balances_data):
    """
    Test case when a field value is not in the current submission but
    was null in the previous submission.
    """
    sub1 = SubmissionAttributes.objects.get(previous_submission__isnull=True)

    quarters = AppropriationAccountBalances.get_quarterly_numbers()
    for q in quarters:
        if q.submission == sub1:
            # qtrly values for year's first submission should remain unchanged
            # (null values become 0 due to COALESCE)
            assert q.budget_authority_unobligated_balance_brought_forward_fyb == Decimal("0.0")
        else:
            # if the equivalent value in the first submisison is NULL, quarterly
            # numbers should equal the number on the 2nd submission
            assert q.budget_authority_unobligated_balance_brought_forward_fyb == Decimal("6.99")


@pytest.mark.skip(reason="expected behavior is not confirmed")
@pytest.mark.django_db
def test_get_quarterly_null_current_submission(tas_balances_data):
    """
    Test case when a field value is NULL in the current submission but
    was not null in the previous submission.
    """


@pytest.mark.django_db
def test_insert_quarterly_numbers(tas_balances_data):
    """
    Test the function that inserts quarterly tas/obj class/pgm activity
    numbers.
    """
    sub1 = SubmissionAttributes.objects.get(previous_submission_id__isnull=True)
    sub2 = SubmissionAttributes.objects.get(previous_submission_id__isnull=False)

    # we start with an empty quarterly table
    quarters = AppropriationAccountBalancesQuarterly.objects.all()
    assert quarters.count() == 0

    # load quarterly records and check results
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers()
    quarters = AppropriationAccountBalancesQuarterly.objects.all()
    assert quarters.count() == 2
    quarter_sub2 = quarters.get(submission=sub2)
    quarter_sub1 = quarters.get(submission=sub1)
    assert quarter_sub2.budget_authority_unobligated_balance_brought_forward_fyb == Decimal("6.99")
    assert quarter_sub2.budget_authority_appropriated_amount_cpe == Decimal("0.00")
    assert quarter_sub2.unobligated_balance_cpe == Decimal(".01")

    # loading again drops and recreates quarterly data
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers()
    quarters = AppropriationAccountBalancesQuarterly.objects.all()
    assert quarters.count() == 2
    assert quarters.get(submission=sub1).id != quarter_sub1.id
    assert quarters.get(submission=sub2).id != quarter_sub2.id

    # load quarterly data for submission 1 only
    quarter_sub2 = quarters.get(submission=sub2)
    quarter_sub1 = quarters.get(submission=sub1)
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers(sub1.submission_id)
    quarters = AppropriationAccountBalancesQuarterly.objects.all()
    assert quarters.count() == 2
    # submission 1 record should be updated
    assert quarters.get(submission=sub1).id != quarter_sub1.id
    # submission 2 record should not be updated
    assert quarters.get(submission=sub2).id == quarter_sub2.id
