# Stdlib imports

# Core Django imports
from django.core.management import call_command
from django.db.models import Q

# Third-party app imports
import pytest
from model_mommy import mommy
from unittest.mock import MagicMock

# Imports from your apps
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.awards.models import Award, FinancialAccountsByAwards, TransactionNormalized


DB_CURSOR_PARAMS = {
    'default': MagicMock(),
    'data_broker': MagicMock(),
    'data_broker_data_file': 'usaspending_api/etl/tests/data/submission_data.json'
}


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_no_d_linkage(mock_db_cursor):
    """
    Test load submission management command for File C records that are not expected to be linked to Award data
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -999
        },
        {
            'model': Award,
            'id': -999,
            'piid': 'RANDOM_LOAD_SUB_PIID_DNE',
            'parent_award_piid': 'PARENT_LOAD_SUB_PIID_DNE',
            'latest_transaction_id': -999
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_piid_with_unmatched_parent_piid(mock_db_cursor):
    """
    Test load submission management command for File C records that are not expected to be linked to Award data
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -1234
        },
        {
            'model': Award,
            'id': -1001,
            'piid': 'RANDOM_LOAD_SUB_PIID',
            'parent_award_piid': 'PARENT_LOAD_SUB_PIID_DNE',
            'latest_transaction_id': -1234
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-1001],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_piid_with_no_parent_piid(mock_db_cursor):
    """
    Test load submission management command for File C records with only a piid and no parent piid
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -998
        },
        {
            'model': Award,
            'id': -998,
            'piid': 'RANDOM_LOAD_SUB_PIID',
            'parent_award_piid': None,
            'latest_transaction_id': -998
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

        call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-998],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_piid_with_parent_piid(mock_db_cursor):
    """
    Test load submission management command for File C records with only a piid and parent piid
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -997
        },
        {
            'model': Award,
            'id': -997,
            'piid': 'RANDOM_LOAD_SUB_PIID',
            'parent_award_piid': 'RANDOM_LOAD_SUB_PARENT_PIID',
            'latest_transaction_id': -997
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

        call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-997, -997],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_fain(mock_db_cursor):
    """
    Test load submission management command for File C records with only a FAIN
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -997
        },
        {
            'model': Award,
            'id': -997,
            'fain': 'RANDOM_LOAD_SUB_FAIN',
            'latest_transaction_id': -997
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

        call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-997],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_uri(mock_db_cursor):
    """
    Test load submission management command for File C records with only a URI
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -997
        },
        {
            'model': Award,
            'id': -997,
            'uri': 'RANDOM_LOAD_SUB_URI',
            'latest_transaction_id': -997
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

        call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-997],
    }

    actual_results = {
        'award_ids': list(FinancialAccountsByAwards.objects.
                          filter(award_id__isnull=False).values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_file_c_fain_and_uri(mock_db_cursor):
    """
    Test load submission management command for File C records with FAIN and URI
    """

    models_to_mock = [
        {
            'model': TreasuryAppropriationAccount,
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        },
        {
            'model': TransactionNormalized,
            'id': -999
        },
        {
            'model': TransactionNormalized,
            'id': -1999
        },
        {
            'model': Award,
            'id': -999,
            'fain': 'RANDOM_LOAD_SUB_FAIN_999',
            'uri': 'RANDOM_LOAD_SUB_URI_999',
            'latest_transaction_id': -999
        },
        {
            'model': Award,
            'id': -1999,
            'fain': 'RANDOM_LOAD_SUB_FAIN_1999',
            'uri': 'RANDOM_LOAD_SUB_URI_1999',
            'latest_transaction_id': -1999
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

        call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = {
        'award_ids': [-1999, -999],
    }

    actual_results = {
        'award_ids': sorted(list(FinancialAccountsByAwards.objects.
                                 filter(award_id__isnull=False).values_list('award_id', flat=True)))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_load_submission_transaction_obligated_amount(mock_db_cursor):
    """ Test load submission management command for File C transaction_obligated_amount NaNs """
    mommy.make(
        TreasuryAppropriationAccount,
        **{
            'treasury_account_identifier': -1111,
            'allocation_transfer_agency_id': '999',
            'agency_id': '999',
            'beginning_period_of_availability': '1700-01-01',
            'ending_period_of_availability': '1700-12-31',
            'availability_type_code': '000',
            'main_account_code': '0000',
            'sub_account_code': '0000',
            'tas_rendering_label': '999-999-000-0000-0000'
        }
    )
    call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = 0
    actual_results = FinancialAccountsByAwards.objects.\
        filter(Q(transaction_obligated_amount='NaN') | Q(transaction_obligated_amount=None)).count()

    assert expected_results == actual_results
