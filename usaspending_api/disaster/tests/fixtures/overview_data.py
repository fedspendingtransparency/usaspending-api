import pytest

from model_mommy import mommy
from usaspending_api.references.models import DisasterEmergencyFundCode


@pytest.fixture
def defc_codes():
    return [
        mommy.make("references.DisasterEmergencyFundCode", code="A", group_name="not_covid_19"),
        mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19"),
        mommy.make("references.DisasterEmergencyFundCode", code="N", group_name="covid_19"),
    ]


@pytest.fixture
def basic_ref_data():
    _fy_2021_schedule()
    _fy_2022_schedule()


@pytest.fixture
def late_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=12,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.3,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.0,
    )


@pytest.fixture
def early_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=1,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.2,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.0,
    )


@pytest.fixture
def year_2_gtas_covid(defc_codes):
    _year_2_gtas("M")


@pytest.fixture
def year_2_gtas_covid_2(defc_codes):
    _year_2_gtas("N")


@pytest.fixture
def year_2_gtas_non_covid(defc_codes):
    _year_2_gtas("A")


def _year_2_gtas(code):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2022,
        fiscal_period=1,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code=code,
        budget_authority_appropriation_amount_cpe=0.22,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.0,
    )


@pytest.fixture
def basic_faba(defc_codes):
    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.all().first(),
        transaction_obligated_amount=0.0,
    )


def _fy_2021_schedule():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2021,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        submission_reveal_date="2021-12-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2021,
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        submission_reveal_date="2022-3-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2021,
        submission_fiscal_quarter=3,
        submission_fiscal_month=9,
        submission_reveal_date="2022-6-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2021,
        submission_fiscal_quarter=4,
        submission_fiscal_month=12,
        submission_reveal_date="2022-9-15",
    )


def _fy_2022_schedule():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        submission_reveal_date="2022-12-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        submission_reveal_date="2023-3-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=9,
        submission_reveal_date="2023-6-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=4,
        submission_fiscal_month=12,
        submission_reveal_date="2023-9-15",
    )
