# Stdlib imports

# Core Django imports
from django.core.management import call_command
from django.db.models import Q

# Third-party app imports
import pytest
from model_mommy import mommy
from unittest.mock import MagicMock

# Imports from your apps
from usaspending_api.awards.models import FinancialAccountsByAwards, TreasuryAppropriationAccount


DB_CURSOR_PARAMS = {
    'data_broker': MagicMock(),
    'data_broker_data_file': 'usaspending_api/etl/tests/data/submission_data.json'
}


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
            'sub_account_code': '0000'
        }
    )
    call_command('load_submission', '--noclean', '--nosubawards', '-9999')

    expected_results = 0
    actual_results = FinancialAccountsByAwards.objects.\
        filter(Q(transaction_obligated_amount='NaN') | Q(transaction_obligated_amount=None)).count()

    assert expected_results == actual_results
