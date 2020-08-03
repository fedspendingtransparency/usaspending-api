import pytest

from datetime import datetime, timezone
from model_mommy import mommy
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.fixture
def submissions(db):
    now = datetime.now(timezone.utc)

    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=True,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=True,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=True,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=3,
        submission_fiscal_month=9,
        submission_reveal_date=now,
    )

    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=1,
        submission_fiscal_month=1,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=1,
        submission_fiscal_month=2,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=2,
        submission_fiscal_month=4,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=2,
        submission_fiscal_month=5,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date=now,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=1700,
        submission_fiscal_quarter=3,
        submission_fiscal_month=9,
        submission_reveal_date=now,
    )

    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_fiscal_year=1700,
        reporting_fiscal_quarter=1,
        reporting_fiscal_period=3,
        quarter_format_flag=True,
        toptier_code="000",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        reporting_fiscal_year=1700,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        quarter_format_flag=True,
        toptier_code="000",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        reporting_fiscal_year=1700,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
        toptier_code="000",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=4,
        reporting_fiscal_year=1700,
        reporting_fiscal_quarter=4,
        reporting_fiscal_period=12,
        quarter_format_flag=False,
        toptier_code="000",
    )


def test_fyqp_filter(submissions):
    # Create file A models
    mommy.make("accounts.AppropriationAccountBalances", submission_id=1)
    mommy.make("accounts.AppropriationAccountBalances", submission_id=2)
    mommy.make("accounts.AppropriationAccountBalances", submission_id=4)  # Monthly

    queryset = account_download_filter("account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 1})
    assert queryset.count() == 1

    queryset = account_download_filter("account_balances", AppropriationAccountBalances, {"fy": 1700, "period": 12})
    assert queryset.count() == 1


def test_federal_account_filter(submissions):
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2)

    # Create file A models
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission_id=1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission_id=2)

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"federal_account": fed_acct1.id, "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1


def test_tas_account_filter_later_qtr_treasury(submissions):
    """ Ensure the fiscal year and quarter filter is working, later quarter - treasury_account"""
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="1")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="2")

    # Create file A models
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission_id=1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission_id=3)

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "treasury_account"
    )
    assert queryset.count() == 1


@pytest.mark.django_db
def test_tas_account_filter_later_qtr_award_financial(submissions):
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

    # Create file C models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account=tas1,
        program_activity=prog1,
        object_class=obj_cls1,
        submission_id=1,
        transaction_obligated_amount=33,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account=tas2,
        program_activity=prog2,
        object_class=obj_cls2,
        submission_id=3,
        transaction_obligated_amount=33,
    )

    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"fy": 1700, "quarter": 1}, "treasury_account"
    )
    assert queryset.count() == 1


def test_tas_account_filter_later_qtr_federal(submissions):
    """ Ensure the fiscal year and quarter filter is working, later quarter - federal account"""
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")
    fed_acct2 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1, tas_rendering_label="1")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct2, tas_rendering_label="2")

    # Create file A models
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission_id=1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission_id=3)

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "federal_account"
    )

    # this count is 1 because we only pull account data from the quarter requested
    assert queryset.count() == 1


def test_tas_account_filter_duplciate_tas_account_balances(submissions):
    """ Ensure the fiscal year and quarter filter is working, duplicate tas for account balances """
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)

    # Create file A models
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission_id=1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission_id=3)

    queryset = account_download_filter(
        "account_balances", AppropriationAccountBalances, {"fy": 1700, "quarter": 3}, "federal_account"
    )
    assert queryset.count() == 1


def test_tas_account_filter_duplciate_tas_financial_accounts_program_object(submissions):
    """ Ensure the fiscal year and quarter filter is working, duplicate tas for financial accounts """
    # Create FederalAccount models
    fed_acct1 = mommy.make("accounts.FederalAccount")

    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fed_acct1)

    # Create file B models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        submission_id=1,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        submission_id=3,
    )

    queryset = account_download_filter(
        "object_class_program_activity", FinancialAccountsByProgramActivityObjectClass, {"fy": 1700, "quarter": 3}
    )
    assert queryset.count() == 1


def test_budget_function_filter(submissions):
    """ Ensure the Budget Function filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", budget_function_code="BUD")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", budget_function_code="NOT")

    # Create file B models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas1.treasury_account_identifier,
        submission_id=1,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account_id=tas2.treasury_account_identifier,
        submission_id=1,
    )

    queryset = account_download_filter(
        "object_class_program_activity",
        FinancialAccountsByProgramActivityObjectClass,
        {"budget_function": "BUD", "fy": 1700, "quarter": 1},
    )
    assert queryset.count() == 1


def test_budget_subfunction_filter(submissions):
    """ Ensure the Budget Subfunction filter is working """
    # Create TAS models
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", budget_subfunction_code="SUB")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", budget_subfunction_code="NOT")

    # Create file C models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas1.treasury_account_identifier,
        submission_id=1,
        transaction_obligated_amount=11,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas2.treasury_account_identifier,
        submission_id=1,
        transaction_obligated_amount=22,
    )

    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"budget_subfunction": "SUB", "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1


def test_cgac_agency_filter(submissions):
    """ Ensure the CGAC agency filter is working """
    ta1 = mommy.make("references.ToptierAgency", toptier_agency_id=-9999, toptier_code="CGC")
    ta2 = mommy.make("references.ToptierAgency", toptier_agency_id=-9998, toptier_code="NOT")

    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="NOT", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", agency_id="CGC", funding_toptier_agency=ta2)

    # Create file B models
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", treasury_account=tas1, submission_id=1
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass", treasury_account=tas2, submission_id=1
    )

    # Filter by ToptierAgency (CGAC)
    queryset = account_download_filter(
        "object_class_program_activity",
        FinancialAccountsByProgramActivityObjectClass,
        {"agency": "-9999", "fy": 1700, "quarter": 1},
    )
    assert queryset.count() == 1


def test_frec_agency_filter(submissions):
    """ Ensure the FREC agency filter is working """
    ta1 = mommy.make("references.ToptierAgency", toptier_agency_id=-9998, toptier_code="FAKE")
    ta2 = mommy.make("references.ToptierAgency", toptier_agency_id=-9999, toptier_code="FREC")

    tas1 = mommy.make(
        "accounts.TreasuryAppropriationAccount", agency_id="CGC", fr_entity_code="FAKE", funding_toptier_agency=ta1
    )
    tas2 = mommy.make(
        "accounts.TreasuryAppropriationAccount", agency_id="CGC", fr_entity_code="FREC", funding_toptier_agency=ta2
    )

    # Create file C models
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas1.treasury_account_identifier,
        submission_id=1,
        transaction_obligated_amount=11,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        treasury_account_id=tas2.treasury_account_identifier,
        submission_id=1,
        transaction_obligated_amount=22,
    )

    # Filter by ToptierAgency (FREC)
    queryset = account_download_filter(
        "award_financial", FinancialAccountsByAwards, {"agency": "-9999", "fy": 1700, "quarter": 1}
    )
    assert queryset.count() == 1
