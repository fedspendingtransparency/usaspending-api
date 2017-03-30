from django.core.management import call_command

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards, TransactionAssistance,
    TransactionContract, Transaction)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import LegalEntity, Location, RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes

import pytest


@pytest.fixture()
def endpoint_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')


@pytest.fixture()
@pytest.mark.django_db
def partially_flushed():
    TransactionContract.objects.all().delete()
    SubmissionAttributes.objects.all().delete()
    AppropriationAccountBalances.objects.all().delete()
    FinancialAccountsByProgramActivityObjectClass.objects.all().delete()
    FinancialAccountsByAwards.objects.all().delete()
    TransactionAssistance.objects.all().delete()
    Transaction.objects.all().delete()
    Location.objects.all().delete()
    LegalEntity.objects.all().delete()
    Award.objects.all().delete()


@pytest.mark.django_db
def test_load_submission_command(endpoint_data, partially_flushed):
    """
    Test the submission loader to validate the ETL process
    """
    # Load the RefObjClass and ProgramActivityCode data
    call_command('load_submission', '-1', '--delete', '--test')
    assert SubmissionAttributes.objects.count() == 1
    assert AppropriationAccountBalances.objects.count() == 1
    assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 10
    assert FinancialAccountsByAwards.objects.count() == 11
    for account in FinancialAccountsByAwards.objects.all():
        assert account.transaction_obligated_amount == 6500
        # for testing, data pulled from etl_test_data.json
    assert Location.objects.count() == 4
    assert LegalEntity.objects.count() == 2
    assert Award.objects.count() == 5
    assert Transaction.objects.count() == 2
    assert TransactionContract.objects.count() == 1
    assert TransactionAssistance.objects.count() == 1


@pytest.mark.django_db
def test_load_submission_command_program_activity_uniqueness(endpoint_data, partially_flushed):
    """
    Verify that loaded RefProgramActivities are unique across
    agency as well as program_activity_code
    """

    code_0001s = RefProgramActivity.objects.filter(program_activity_code='0001')
    assert code_0001s.count() == 3
    assert not code_0001s.filter(responsible_agency_id=19).exists()
    call_command('load_submission', '-1', '--delete', '--test')
    assert code_0001s.count() == 4
    assert code_0001s.filter(responsible_agency_id=19).exists()
