import pytest

from django.core.exceptions import FieldError

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.mark.django_db
def test_account_balances_treasury_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["account_balances"]["treasury_account"].values()
        account_download_filter(
            "account_balances", AppropriationAccountBalances, {"fy": 2017, "quarter": 4}, "treasury_account"
        ).values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_account_balances_federal_account_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["account_balances"]["federal_account"].values()
        account_download_filter(
            "account_balances", AppropriationAccountBalances, {"fy": 2017, "quarter": 4}, "federal_account"
        ).values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_treasury_account_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["object_class_program_activity"]["treasury_account"].values()
        account_download_filter(
            "object_class_program_activity",
            FinancialAccountsByProgramActivityObjectClass,
            {"fy": 2017, "quarter": 4},
            "treasury_account",
        ).values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_federal_account_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["object_class_program_activity"]["federal_account"].values()
        account_download_filter(
            "object_class_program_activity",
            FinancialAccountsByProgramActivityObjectClass,
            {"fy": 2017, "quarter": 4},
            "federal_account",
        ).values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_award_financial_treasury_account_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["award_financial"]["treasury_account"].values()
        account_download_filter(
            "award_financial", FinancialAccountsByAwards, {"fy": 2017, "quarter": 4}, "treasury_account"
        ).values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_award_financial_federal_account_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths["award_financial"]["federal_account"].values()
        account_download_filter(
            "award_financial", FinancialAccountsByAwards, {"fy": 2017, "quarter": 4}, "federal_account"
        ).values(*query_values)
    except FieldError:
        assert False
