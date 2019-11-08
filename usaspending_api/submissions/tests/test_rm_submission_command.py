import pytest

from model_mommy import mommy
from django.core.management import call_command
from django.db.models import Q

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.accounts.models import AppropriationAccountBalances, AppropriationAccountBalancesQuarterly
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
    TasProgramActivityObjectClassQuarterly,
)

SUBMISSION_MODELS = [
    AppropriationAccountBalances,
    FinancialAccountsByAwards,
    FinancialAccountsByProgramActivityObjectClass,
    TasProgramActivityObjectClassQuarterly,
    AppropriationAccountBalancesQuarterly,
]


@pytest.fixture
def submission_data():
    submission_123 = mommy.make("submissions.SubmissionAttributes", broker_submission_id=123)
    submission_456 = mommy.make("submissions.SubmissionAttributes", broker_submission_id=456)

    mommy.make("accounts.AppropriationAccountBalances", submission=submission_123, _quantity=10)
    mommy.make("awards.FinancialAccountsByAwards", submission=submission_123, _quantity=10)
    loc1 = mommy.make("references.Location", city_name="city1")
    loc2 = mommy.make("references.Location", city_name="city2")
    # Making child transaction items creates the parent by default
    mommy.make(
        "awards.TransactionFABS",
        transaction__place_of_performance=loc1,
        transaction__award__piid="ABC123",
        transaction__award__parent_award__piid="DEF455",
        _quantity=10,
    )
    mommy.make("awards.TransactionFPDS", transaction__place_of_performance=loc2, _quantity=10)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", submission=submission_123, _quantity=10
    )
    mommy.make("financial_activities.TasProgramActivityObjectClassQuarterly", submission=submission_123, _quantity=10)
    mommy.make("accounts.AppropriationAccountBalancesQuarterly", submission=submission_123, _quantity=10)

    mommy.make("accounts.AppropriationAccountBalances", submission=submission_456, _quantity=10)
    mommy.make("awards.FinancialAccountsByAwards", submission=submission_456, _quantity=10)
    loc3 = mommy.make("references.Location", city_name="city3")
    # Making child transaction items creates the parent by default
    mommy.make("awards.TransactionFPDS", transaction__place_of_performance=loc2, _quantity=10)
    mommy.make("awards.TransactionFABS", transaction__place_of_performance=loc3, _quantity=10)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", submission=submission_456, _quantity=10
    )
    mommy.make("financial_activities.TasProgramActivityObjectClassQuarterly", submission=submission_456, _quantity=10)
    mommy.make("accounts.AppropriationAccountBalancesQuarterly", submission=submission_456, _quantity=10)


@pytest.mark.django_db
def test_verify_fixture(client, submission_data):
    # Verify our db is set up properly
    verify_zero_count(SUBMISSION_MODELS, 123, eq_zero=False)
    verify_zero_count(SUBMISSION_MODELS, 456, eq_zero=False)


@pytest.mark.django_db
def test_rm_submission(client, submission_data):
    call_command("rm_submission", 123)

    verify_zero_count(SUBMISSION_MODELS, 123)

    # Make sure we still have our other submission
    verify_zero_count(SUBMISSION_MODELS, 456, eq_zero=False)

    call_command("rm_submission", 456)

    verify_zero_count(SUBMISSION_MODELS, 456)


# @pytest.mark.django_db
# def test_rm_submission_location_cleanup(client, submission_data):
#     """Locations no longer referring to a record should be cleaned up."""
#
#     assert Location.objects.count() == 3
#     call_command("rm_submission", 123)
#     # city 1 appears only in submission 123 and should be gone
#     assert Location.objects.count() == 2
#     assert not Location.objects.filter(city_name='city1').exists()
#     # city 2 also appears in submission 456 and should still be present
#     assert Location.objects.filter(city_name='city2').exists()
#
#     call_command("rm_submission", 456)
#     assert not Location.objects.exists()


def verify_zero_count(models, submission_id, field="submission", eq_zero=True):
    q_kwargs = {}
    q_kwargs[field + "__broker_submission_id"] = submission_id
    q_obj = Q(**q_kwargs)
    print(q_obj)
    for model in models:
        if eq_zero:
            assert model.objects.filter(q_obj).count() == 0
        else:
            assert model.objects.filter(q_obj).count() != 0
