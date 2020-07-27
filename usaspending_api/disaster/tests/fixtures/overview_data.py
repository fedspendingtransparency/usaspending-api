import pytest

from datetime import date
from model_mommy import mommy
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.disaster.v2.views.disaster_base import COVID_19_GROUP_NAME

NOT_COVID_NAME = "not_covid_19"

EARLY_MONTH = 3
LATE_MONTH = 12


@pytest.fixture
def defc_codes():
    return [
        mommy.make("references.DisasterEmergencyFundCode", code="A", group_name=NOT_COVID_NAME),
        mommy.make("references.DisasterEmergencyFundCode", code="M", group_name=COVID_19_GROUP_NAME),
        mommy.make("references.DisasterEmergencyFundCode", code="N", group_name=COVID_19_GROUP_NAME),
    ]


@pytest.fixture
def basic_ref_data():
    _fy_2021_schedule()
    _fy_2022_schedule()


@pytest.fixture
def partially_completed_year():
    _incomplete_schedule_for_year(2021)


@pytest.fixture
def late_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.3,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.03,
    )


@pytest.fixture
def quarterly_gtas(defc_codes):
    _partial_quarterly_schedule_for_year(2021)
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=9,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.26,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.02,
    )


@pytest.fixture
def early_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.2,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.02,
    )


@pytest.fixture
def non_covid_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="A",
        budget_authority_appropriation_amount_cpe=0.32,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.13,
    )


@pytest.fixture
def unobligated_balance_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=1.5,
        disaster_emergency_fund_code="A",
        budget_authority_appropriation_amount_cpe=0.75,
        other_budgetary_resources_amount_cpe=0.75,
        gross_outlay_amount_by_tas_cpe=0.0,
    )


@pytest.fixture
def other_budget_authority_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2021,
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.7,
        other_budgetary_resources_amount_cpe=0.15,
        gross_outlay_amount_by_tas_cpe=0.02,
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
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=0.1,
        disaster_emergency_fund_code=code,
        budget_authority_appropriation_amount_cpe=0.32,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.07,
    )


@pytest.fixture
def basic_faba(defc_codes):
    submission = mommy.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2021, reporting_fiscal_period=EARLY_MONTH
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.all().first(),
        transaction_obligated_amount=0.0,
        submission=submission,
    )


@pytest.fixture
def faba_with_values(defc_codes):
    _year_1_faba(1.6, "M")
    _year_1_faba(0.7, "M")


@pytest.fixture
def faba_with_non_covid_values(defc_codes):
    _year_1_faba(1.6, "M")
    _year_1_faba(0.7, "A")


@pytest.fixture
def multi_year_faba(defc_codes):
    _year_1_faba(1.6, "M")
    _year_2_faba_with_value(0.7)


@pytest.fixture
def multi_period_faba(defc_codes):
    _year_2_late_faba_with_value(1.6)
    _year_2_faba_with_value(0.7)


def _year_1_faba(value, code):
    submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2021,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        reporting_period_start=date(2021, LATE_MONTH, 1),
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code=code).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0,
        submission=submission,
    )


def _year_2_faba_with_value(value):
    submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=EARLY_MONTH,
        quarter_format_flag=False,
        reporting_period_start=date(2022, EARLY_MONTH, 1),
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0,
        submission=submission,
    )


def _year_2_late_faba_with_value(value):
    submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        reporting_period_start=date(2022, LATE_MONTH, 1),
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0,
        submission=submission,
    )


def _fy_2021_schedule():
    _full_schedule_for_year(2021)


def _fy_2022_schedule():
    _full_schedule_for_year(2022)


def _full_schedule_for_year(year):
    for month in range(1, LATE_MONTH + 1):
        mommy.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=False,
            submission_fiscal_year=year,
            submission_fiscal_quarter=(month / 3) + 1,
            submission_fiscal_month=month,
            submission_reveal_date=f"{year}-{month}-15",
        )


def _partial_quarterly_schedule_for_year(year):
    for quarter in range(1, 4):
        mommy.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=True,
            submission_fiscal_year=year,
            submission_fiscal_quarter=quarter,
            submission_fiscal_month=quarter * 3,
            submission_reveal_date=f"{year}-{quarter * 3}-15",
        )


def _incomplete_schedule_for_year(year):
    for month in range(1, EARLY_MONTH + 1):
        mommy.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=False,
            submission_fiscal_year=year,
            submission_fiscal_quarter=(month / 3) + 1,
            submission_fiscal_month=month,
            submission_reveal_date=f"{year}-{month}-15",
        )
