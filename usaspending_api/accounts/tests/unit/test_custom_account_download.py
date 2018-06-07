import pytest

from django.core.exceptions import FieldError

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.mark.django_db
def test_account_balances_tas_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['account_balances']['treasury_account'].values()
        AppropriationAccountBalances.objects.values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_tas_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['object_class_program_activity']['treasury_account'].values()
        FinancialAccountsByProgramActivityObjectClass.objects.values(*query_values)
    except FieldError:
        assert False


@pytest.mark.django_db
def test_award_financial_tas_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        query_values = query_paths['award_financial']['treasury_account'].values()
        FinancialAccountsByAwards.objects.values(*query_values)
    except FieldError:
        assert False
