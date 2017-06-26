import datetime
from decimal import Decimal
from django.core.management import call_command, CommandError

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards, TransactionAssistance,
    TransactionContract, Transaction)
from usaspending_api.etl.management.commands.load_submission import get_submission_attributes, get_or_create_program_activity
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
def test_load_historical_command_contracts(endpoint_data, partially_flushed):
    """
    Test process to load detached historical contract info, not part of a submission
    """
    assert Award.objects.count() == 0
    assert TransactionContract.objects.count() == 0
    assert LegalEntity.objects.count() == 0
    assert Location.objects.count() == 0
    call_command('load_historical', '--test', '--contracts',
                 '--action_date_begin', '2017-05-01',
                 '--action_date_end', '2017-05-02',
                 '--cgac', '015')
    assert Award.objects.count() == 9
    assert TransactionContract.objects.count() == 10
    assert LegalEntity.objects.count() == 9
    assert Location.objects.count() == 10

    # verify that load is idempotent by running it again, verifying extra records not created
    call_command('load_historical', '--test', '--contracts',
                 '--action_date_begin', '2017-05-01',
                 '--action_date_end', '2017-05-02',
                 '--cgac', '015')
    assert Award.objects.count() == 9
    assert TransactionContract.objects.count() == 10
    assert LegalEntity.objects.count() == 9
    assert Location.objects.count() == 10


@pytest.mark.skip("detached_award_financial_assistance lacks a state column")
@pytest.mark.django_db
def test_load_historical_command_financial_assistance(endpoint_data, partially_flushed):
    """
    Test historical loader for financial assistance
    """
    assert Award.objects.count() == 0
    assert TransactionAssistance.objects.count() == 0
    assert LegalEntity.objects.count() == 0
    assert Location.objects.count() == 0
    call_command('load_historical', '--test', '--financial_assistance',
                 '--action_date_begin', '2016-04-01',
                 '--action_date_end', '2016-04-30',
                 '--awarding_agency_code', '031')
    assert Award.objects.count() == 9
    assert TransactionAssistance.objects.count() == 10
    assert LegalEntity.objects.count() == 9
    assert Location.objects.count() == 10


@pytest.mark.django_db
def test_load_submission_command(endpoint_data, partially_flushed):
    """
    Test the submission loader to validate the ETL process
    """
    # Load the RefObjClass and ProgramActivityCode data
    call_command('load_submission', '-1', '--test')
    assert SubmissionAttributes.objects.count() == 1
    assert AppropriationAccountBalances.objects.count() == 1
    assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 9
    assert FinancialAccountsByAwards.objects.count() == 11
    for account in FinancialAccountsByAwards.objects.all():
        assert account.transaction_obligated_amount == -6500
        # for testing, data pulled from etl_test_data.json
    assert Location.objects.count() == 4
    assert LegalEntity.objects.count() == 2
    assert Award.objects.count() == 7
    assert Transaction.objects.count() == 2
    assert TransactionContract.objects.count() == 1
    assert TransactionAssistance.objects.count() == 1

    # Verify that sign has been reversed during load where appropriate
    assert AppropriationAccountBalances.objects.filter(gross_outlay_amount_by_tas_cpe__lt=0).count() == 1
    assert FinancialAccountsByProgramActivityObjectClass.objects.filter(obligations_delivered_orders_unpaid_total_cpe__lt=0).count() == 9
    assert FinancialAccountsByProgramActivityObjectClass.objects.filter(obligations_delivered_orders_unpaid_total_fyb__lt=0).count() == 9
    assert FinancialAccountsByAwards.objects.filter(gross_outlay_amount_by_award_cpe__lt=0).count() == 11
    assert FinancialAccountsByAwards.objects.filter(gross_outlay_amount_by_award_fyb__lt=0).count() == 11
    assert FinancialAccountsByAwards.objects.filter(gross_outlay_amount_by_award_fyb__lt=0).count() == 11
    assert FinancialAccountsByAwards.objects.filter(transaction_obligated_amount__lt=0).count() == 11

    # Verify that duplicate 4 digit object class rows in File B have been combined
    combined_b = FinancialAccountsByProgramActivityObjectClass.objects.filter(
        object_class__object_class='255',
        object_class__direct_reimbursable='2',
        program_activity__program_activity_code='0001'
    )
    assert combined_b.count() == 1
    combined_b = combined_b.first()
    assert combined_b.obligations_undelivered_orders_unpaid_total_fyb == Decimal('-3333.00')
    assert combined_b.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe == Decimal('0.00')
    assert combined_b.deobligations_recoveries_refund_pri_program_object_class_cpe == Decimal('-3200.00')


@pytest.mark.django_db
def test_get_submission_attributes():
    submission_data = {
        'cgac_code': 'ABC',
        'reporting_fiscal_year': 2016,
        'reporting_fiscal_period': 9,
        'is_quarter_format': True,
        'reporting_start_date': datetime.date(2016, 4, 1),
        'reporting_end_date': datetime.date(2016, 6, 1),
        'updated_at': datetime.datetime(2017, 4, 20, 16, 49, 28, 915209)
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
    assert sub.reporting_period_start == datetime.date(2016, 4, 1)
    assert sub.reporting_period_end == datetime.date(2016, 6, 1)
    assert sub.previous_submission is None
    assert sub.certified_date == datetime.date(2017, 4, 20)

    # test re-running the submission
    old_create_date = sub.create_date
    submission_data['cgac_code'] = 'XYZ'
    sub = get_submission_attributes(11111, submission_data)
    # there should only be one submission in the db right now
    assert SubmissionAttributes.objects.all().count() == 1
    # submission record has been deleted and replaced
    sub = SubmissionAttributes.objects.get(broker_submission_id=11111)
    assert sub.create_date > old_create_date
    # record should have the updated cgac info
    assert sub.cgac_code == 'XYZ'

    # insert a submission for the following quarter
    new_submission_data = {
        'cgac_code': 'XYZ',
        'reporting_fiscal_year': 2016,
        'reporting_fiscal_period': 12,
        'is_quarter_format': True,
        'reporting_start_date': datetime.date(2016, 7, 1),
        'reporting_end_date': datetime.date(2016, 9, 1),
        'updated_at': datetime.datetime(2017, 4, 20, 16, 49, 28, 915209)
    }
    get_submission_attributes(22222, new_submission_data)

    # there should now be two submissions in the db
    assert SubmissionAttributes.objects.all().count() == 2
    sub2 = SubmissionAttributes.objects.get(broker_submission_id=22222)
    # newer submission should recognize the first submission as it's previous sub
    assert sub2.previous_submission == sub
    # trying to replace the first submission should fail now that it has
    # a "downstream" submission
    with pytest.raises(ValueError):
        get_submission_attributes(11111, new_submission_data)


@pytest.mark.django_db
def test_load_submission_command_program_activity_uniqueness(endpoint_data, partially_flushed):
    """
    Verify that loaded RefProgramActivities are unique across
    agency as well as program_activity_code
    """

    code_0001s = RefProgramActivity.objects.filter(program_activity_code='0001')
    assert code_0001s.count() == 3
    assert not code_0001s.filter(responsible_agency_id='019').exists()
    call_command('load_submission', '-1', '--test')
    assert code_0001s.count() == 4
    assert code_0001s.filter(responsible_agency_id='019').exists()


@pytest.mark.django_db
def test_get_or_create_program_activity_name(transaction=True):
    """
    Verify that program activities that aren't in our domain values will store
    both a code and display name if it's present in the submission data
    """
    row_data = {
        'budget_year': 2017,
        'agency_identifier': '999',
        'main_account_code': '9999',
        'program_activity_code': '9999',
        'program_activity_name': 'PA that is not in our list'
    }
    # insert a submission for the following quarter
    new_submission_data = {
        'cgac_code': '999',
        'reporting_fiscal_year': 2017,
        'reporting_fiscal_period': 2,
        'is_quarter_format': True,
        'reporting_start_date': datetime.date(2017, 7, 1),
        'reporting_end_date': datetime.date(2017, 9, 1),
        'updated_at': datetime.datetime(2017, 4, 20, 16, 49, 28, 915209),
    }
    sa = get_submission_attributes(22222, new_submission_data)
    pa = get_or_create_program_activity(row_data, sa)
    assert pa.program_activity_name == 'PA that is not in our list'
