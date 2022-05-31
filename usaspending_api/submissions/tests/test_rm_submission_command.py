import pytest

from model_bakery import baker
from django.core.management import call_command
from django.db.models import Q

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass

SUBMISSION_MODELS = [
    AppropriationAccountBalances,
    FinancialAccountsByAwards,
    FinancialAccountsByProgramActivityObjectClass,
]


@pytest.fixture
def submission_data():
    submission_123 = baker.make("submissions.SubmissionAttributes", submission_id=123)
    submission_456 = baker.make("submissions.SubmissionAttributes", submission_id=456)

    baker.make("accounts.AppropriationAccountBalances", submission=submission_123, _quantity=10)
    baker.make("awards.FinancialAccountsByAwards", submission=submission_123, _quantity=10)

    # Making child transaction items creates the parent by default
    baker.make(
        "awards.TransactionFABS",
        transaction__award__piid="ABC123",
        transaction__award__parent_award__piid="DEF455",
        place_of_performance_city="city1",
        _quantity=10,
    )
    baker.make("awards.TransactionFPDS", place_of_perform_city_name="city2", _quantity=10)
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", submission=submission_123, _quantity=10
    )
    baker.make("accounts.AppropriationAccountBalances", submission=submission_456, _quantity=10)
    baker.make("awards.FinancialAccountsByAwards", submission=submission_456, _quantity=10)

    # Making child transaction items creates the parent by default
    baker.make("awards.TransactionFPDS", place_of_perform_city_name="city2", _quantity=10)
    baker.make("awards.TransactionFABS", place_of_performance_city="city3", _quantity=10)
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", submission=submission_456, _quantity=10
    )


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


def verify_zero_count(models, submission_id, field="submission", eq_zero=True):
    q_kwargs = {}
    q_kwargs[field + "__submission_id"] = submission_id
    q_obj = Q(**q_kwargs)
    for model in models:
        if eq_zero:
            assert model.objects.filter(q_obj).count() == 0
        else:
            assert model.objects.filter(q_obj).count() != 0
