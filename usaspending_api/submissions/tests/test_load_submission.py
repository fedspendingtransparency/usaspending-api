import pytest
from django.core.management import call_command

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    FinancialAccountsByAwardsTransactionObligations, FinancialAssistanceAward,
    Procurement)
from usaspending_api.common.helpers import fy
from usaspending_api.financial_activities.models import \
    FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import LegalEntity, Location
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture()
def endpoint_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')


@pytest.fixture()
@pytest.mark.django_db
def partially_flushed():
    Procurement.objects.all().delete()
    SubmissionAttributes.objects.all().delete()
    AppropriationAccountBalances.objects.all().delete()
    FinancialAccountsByProgramActivityObjectClass.objects.all().delete()
    FinancialAccountsByAwards.objects.all().delete()
    FinancialAccountsByAwardsTransactionObligations.objects.all().delete()
    FinancialAssistanceAward.objects.all().delete()
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
    assert FinancialAccountsByAwardsTransactionObligations.objects.count(
    ) == 11
    assert Location.objects.count() == 4
    assert LegalEntity.objects.count() == 2
    assert Award.objects.count() == 7
    assert Procurement.objects.count() == 1
    assert FinancialAssistanceAward.objects.count() == 1


@pytest.mark.django_db
def test_load_submission_correct_fiscal_years(endpoint_data,
                                              partially_flushed):
    call_command('load_submission', '-1', '--delete', '--test')
    result = FinancialAccountsByAwards.objects.first()
    assert fy(
        result.reporting_period_start) == result.reporting_period_start_fy
    assert fy(result.reporting_period_end) == result.reporting_period_end_fy
    assert fy(result.certified_date) == result.certified_date_fy
