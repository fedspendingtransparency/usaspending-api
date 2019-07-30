import pytest

from model_mommy import mommy

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.mark.django_db
def test_fyq_filter():
    """ Ensure the fiscal year and quarter filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount")

    # Create file A models
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas2,
        reporting_period_start="1700-01-01",
        reporting_period_end="1700-03-31",
        final_of_fy=True,
    )

    queryset = account_download_filter("account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 1})
    assert queryset.count() == 1


@pytest.mark.django_db
def test_federal_account_filter():
    """ Ensure the fiscal year and quarter filter is working """
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2)

    # Create file A models
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas2,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"federal_account": fed_acct1.id, "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_tas_account_filter_later_qtr_treasury():
    """ Ensure the fiscal year and quarter filter is working, later quarter - treasury_account"""
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1, tas_rendering_label="1")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2, tas_rendering_label="2")

    # Create file A models
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas2,
        reporting_period_start="1700-04-01",
        reporting_period_end="1700-06-30",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "treasury_account"
    )
    assert queryset.count() == 2


@pytest.mark.django_db
def test_tas_account_filter_later_qtr_award_financial():
    """ Ensure the fiscal year and quarter filter is working, later quarter - award_financial"""
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create Program Activities
    prog1 = mommy.make("references.RefProgramActivity", program_activity_code="0001")
    prog2 = mommy.make("references.RefProgramActivity", program_activity_code="0002")

    # Create Object Classes
    obj_cls1 = mommy.make("references.ObjectClass", object_class="001")
    obj_cls2 = mommy.make("references.ObjectClass", object_class="002")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1, tas_rendering_label="1")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2, tas_rendering_label="2")

    # Create file A models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        program_activity=prog1,
        object_class=obj_cls1,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account=tas2,
        reporting_period_start="1700-04-01",
        reporting_period_end="1700-06-30",
        program_activity=prog2,
        object_class=obj_cls2,
    )

    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"fy": 1700, "quarter": 3}, "treasury_account"
    )
    assert queryset.count() == 2


@pytest.mark.django_db
def test_tas_account_filter_later_qtr_federal():
    """ Ensure the fiscal year and quarter filter is working, later quarter - federal account"""
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1, tas_rendering_label="1")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2, tas_rendering_label="2")

    # Create file A models
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas2,
        reporting_period_start="1700-04-01",
        reporting_period_end="1700-06-30",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "federal_account"
    )
    # this count is 2 as the federal account downloads are now cumulative while still excluding duplicates
    assert queryset.count() == 2


@pytest.mark.django_db
def test_tas_account_filter_duplciate_tas_account_balances():
    """ Ensure the fiscal year and quarter filter is working, duplicate tas for account balances """
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)

    # Create file A models
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=False,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas1,
        reporting_period_start="1700-04-01",
        reporting_period_end="1700-06-30",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "federal_account"
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_tas_account_filter_duplciate_tas_financial_accounts_program_object():
    """ Ensure the fiscal year and quarter filter is working, duplicate tas for financial accounts """
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)

    # Create file A models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=False,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1700-04-01",
        reporting_period_end="1700-06-30",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "object_class_program_activity", FinancialAccountsByProgramActivityObjectClass, {"fy": 1700, "quarter": 3}
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_budget_function_filter():
    """ Ensure the Budget Function filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", budget_function_code="BUD")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", budget_function_code="NOT")

    # Create file B models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas2.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )

    queryset = account_download_filter(
        "object_class_program_activity",
        FinancialAccountsByProgramActivityObjectClass,
        {"budget_function": "BUD", "fy": 1700, "quarter": 1},
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_budget_subfunction_filter():
    """ Ensure the Budget Subfunction filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", budget_subfunction_code="SUB")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", budget_subfunction_code="NOT")

    # Create file C models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas2.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
    )

    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"budget_subfunction": "SUB", "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_cgac_agency_filter():
    """ Ensure the CGAC agency filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="NOT")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="CGC")

    # Create file B models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas2.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
        final_of_fy=True,
    )

    # Create ToptierAgency models
    mommy.make("references.ToptierAgency", toptier_agency_id=-9999, cgac_code="CGC")
    mommy.make("references.ToptierAgency", toptier_agency_id=-9998, cgac_code="NOT")

    # Filter by ToptierAgency (CGAC)
    queryset = account_download_filter(
        "object_class_program_activity",
        FinancialAccountsByProgramActivityObjectClass,
        {"agency": "-9999", "fy": 1700, "quarter": 1},
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_frec_agency_filter():
    """ Ensure the FREC agency filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="CGC", fr_entity_code="FAKE")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="CGC", fr_entity_code="FREC")

    # Create file C models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas1.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas2.treasury_account_identifier,
        reporting_period_start="1699-10-01",
        reporting_period_end="1699-12-31",
    )

    # Create ToptierAgency models
    mommy.make("references.ToptierAgency", toptier_agency_id=-9999, cgac_code="FREC")
    mommy.make("references.ToptierAgency", toptier_agency_id=-9998, cgac_code="FAKE")
    mommy.make("references.ToptierAgency", toptier_agency_id=-9997, cgac_code="CGC")

    # Filter by ToptierAgency (FREC)
    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"agency": "-9999", "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1
