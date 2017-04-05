from datetime import date

from django.core.management import call_command

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards, TransactionAssistance,
    TransactionContract, Transaction)
from usaspending_api.etl.management.commands.load_submission import get_submission_attributes
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
    assert Award.objects.count() == 7
    assert Transaction.objects.count() == 2
    assert TransactionContract.objects.count() == 1
    assert TransactionAssistance.objects.count() == 1


@pytest.mark.django_db
def test_get_get_submission_attributes():
    submission_data = {
        'cgac_code': 'ABC',
        'reporting_fiscal_year': 2016,
        'reporting_fiscal_period': 9,
        'is_quarter_format': True,
        'reporting_start_date': date(2016, 4, 1),
        'reporting_end_date': date(2016, 6, 1),
    }

    get_submission_attributes(11111, submission_data)
    # there should only be one submission in the db right now
    assert SubmissionAttributes.objects.all().count() == 1

    # get the submission and make sure all fields populated correctly
    sub = SubmissionAttributes.objects.get(broker_submission_id=11111)
    assert sub.cgac_code == 'ABC'
    assert sub.reporting_fiscal_year == 2016
    assert sub.reporting_fiscal_period == 9
    assert sub.quarter_format_flag is True
    assert sub.reporting_period_start == date(2016, 4, 1)
    assert sub.reporting_period_end == date(2016, 6, 1)
    assert sub.previous_submission is None

    # change submission data but don't send --delete directive
    old_submission_id = sub.submission_id
    submission_data['cgac_code'] = 'XYZ'
    get_submission_attributes(11111, submission_data)
    sub = SubmissionAttributes.objects.get(broker_submission_id=11111)
    # this is the same submission record we created previously
    assert sub.submission_id == old_submission_id
    # record should have the update cgac info
    assert sub.cgac_code == 'XYZ'

    # test the --delete directive
    get_submission_attributes(11111, submission_data, True)
    # there should only be one submission in the db right now
    assert SubmissionAttributes.objects.all().count() == 1
    # submission record has been deleted and replaced
    sub = SubmissionAttributes.objects.get(broker_submission_id=11111)
    assert sub.submission_id != old_submission_id

    # insert a submission for the following quarter
    new_submission_data = {
        'cgac_code': 'XYZ',
        'reporting_fiscal_year': 2016,
        'reporting_fiscal_period': 12,
        'is_quarter_format': True,
        'reporting_start_date': date(2016, 7, 1),
        'reporting_end_date': date(2016, 9, 1),
    }
    get_submission_attributes(22222, new_submission_data, True)

    # there should now be two submissions in the db
    assert SubmissionAttributes.objects.all().count() == 2
    sub2 = SubmissionAttributes.objects.get(broker_submission_id=22222)
    # newer submission should recognize the first submission as it's previous sub
    assert sub2.previous_submission == sub
    # trying to delete the first submission should fail now that it has
    # a "downstream" submission
    with pytest.raises(ValueError):
        get_submission_attributes(11111, new_submission_data, True)


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
