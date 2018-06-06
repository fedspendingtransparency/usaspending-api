import pytest

from django.core.exceptions import FieldError

from usaspending_api.accounts.v2.filters.account_balances import account_balances_filter
from usaspending_api.accounts.v2.filters.object_class_program_activity import object_class_program_activity_filter
from usaspending_api.accounts.v2.filters.award_financial import award_financial_filter
from usaspending_api.download.v2.download_column_historical_lookups import query_paths

BASE_ACCOUNT_DOWNLOAD_FILTER = {'fy': 2017, 'quarter': 4}


@pytest.mark.django_db
def test_account_balances_treasury_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['account_balances']['treasury_account'].values()
        account_balances_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'treasury_account').values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_account_balances_federal_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['account_balances']['federal_account'].values()
        account_balances_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'federal_account').values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_treasury_account_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['object_class_program_activity']['treasury_account'].values()
        object_class_program_activity_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'treasury_account').values(*query_values)
    except FieldError:
        assert False


# @pytest.mark.django_db
# def test_object_class_program_activity_federal_account_mapping():
#     """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
#     try:
#         query_values = query_paths['object_class_program_activity']['federal_account'].values()
#         object_class_program_activity_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'federal_account').values(*query_values)
#     except FieldError:
#         assert False


@pytest.mark.django_db
def test_award_financial_treasury_account_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['award_financial']['treasury_account'].values()
        award_financial_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'treasury_account').values(*query_values)
    except FieldError:
        assert False


# @pytest.mark.django_db
# def test_award_financial_federal_account_mapping():
#     """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
#     try:
#         query_values = query_paths['award_financial']['federal_account'].values()
#         award_financial_filter(BASE_ACCOUNT_DOWNLOAD_FILTER, 'federal_account').values(*query_values)
#     except FieldError:
#         assert False
