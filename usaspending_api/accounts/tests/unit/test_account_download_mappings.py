import pytest

from django.core.exceptions import FieldError

from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths

BASE_FILTERS = {'fy': 2017, 'quarter': 4}
A_MODEL = VALUE_MAPPINGS['account_balances']['table']
B_MODEL = VALUE_MAPPINGS['object_class_program_activity']['table']
C_MODEL = VALUE_MAPPINGS['award_financial']['table']


@pytest.mark.django_db
def test_account_balances_treasury_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['account_balances']['treasury_account'].values()
        account_download_filter('account_balances', A_MODEL, BASE_FILTERS, 'treasury_account').values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_account_balances_federal_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['account_balances']['federal_account'].values()
        account_download_filter('account_balances', A_MODEL, BASE_FILTERS, 'federal_account').values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_treasury_account_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['object_class_program_activity']['treasury_account'].values()
        account_download_filter('object_class_program_activity', B_MODEL, BASE_FILTERS, 'treasury_account').\
            values(*query_values)
    except FieldError:
        assert False


# @pytest.mark.django_db
# def test_object_class_program_activity_federal_account_mapping():
#     """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
#     try:
#         query_values = query_paths['object_class_program_activity']['federal_account'].values()
#         account_download_filter('object_class_program_activity', B_MODEL, BASE_FILTERS, 'federal_account').\
#             values(*query_values)
#     except FieldError:
#         assert False


@pytest.mark.django_db
def test_award_financial_treasury_account_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['award_financial']['treasury_account'].values()
        account_download_filter('award_financial', C_MODEL, BASE_FILTERS, 'treasury_account').values(*query_values)
    except FieldError:
        assert False


# @pytest.mark.django_db
# def test_award_financial_federal_account_mapping():
#     """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
#     try:
#         query_values = query_paths['award_financial']['federal_account'].values()
#         account_download_filter('award_financial', C_MODEL, BASE_FILTERS, 'federal_account').values(*query_values)
#     except FieldError:
#         assert False
