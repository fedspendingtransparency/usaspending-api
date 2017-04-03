from datetime import date

from django.core.management import call_command

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards, TransactionAssistance,
    TransactionContract, Transaction)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import LegalEntity, Location
from usaspending_api.submissions.models import SubmissionAttributes

import pytest


@pytest.fixture(scope="module")
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

    # Check specific submission attributes to make sure we're loading what
    # we need from the data broker
    sub = SubmissionAttributes.objects.all().first()
    assert sub.broker_submission_id == -1
    assert sub.reporting_fiscal_year == 2016
    assert sub.reporting_fiscal_period == 3
    assert sub.reporting_fiscal_quarter == 1
    assert sub.reporting_period_start == date(2015, 10, 1)
    assert sub.reporting_period_end == date(2015, 12, 31)
