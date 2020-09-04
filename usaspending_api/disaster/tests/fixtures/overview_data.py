import pytest

from datetime import date
from model_mommy import mommy
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.disaster.v2.views.disaster_base import COVID_19_GROUP_NAME

NOT_COVID_NAME = "not_covid_19"

EARLY_MONTH = 3
LATE_MONTH = 12

EARLY_YEAR = 2021
LATE_YEAR = 2022

LATE_GTAS_BUDGETARY_RESOURCES = 0.3
LATE_GTAS_UNOBLIGATED_BALANCE = 0.0
LATE_GTAS_APPROPRIATION = 0.29
LATE_GTAS_OUTLAY = 0.03

QUARTERLY_GTAS_BUDGETARY_RESOURCES = 0.26

EARLY_GTAS_BUDGETARY_RESOURCES = 0.20
EARLY_GTAS_OUTLAY = 0.02

UNOBLIGATED_GTAS_BUDGETARY_RESOURCES = 1.5

YEAR_TWO_GTAS_BUDGETARY_RESOURCES = 0.32
YEAR_TWO_GTAS_UNOBLIGATED_BALANCE = 0.1
YEAR_TWO_GTAS_APPROPRIATION = 0.31
YEAR_TWO_OUTLAY = 0.07


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
    _incomplete_schedule_for_year(EARLY_YEAR)


@pytest.fixture
def late_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        total_budgetary_resources_cpe=LATE_GTAS_BUDGETARY_RESOURCES,
        budget_authority_appropriation_amount_cpe=LATE_GTAS_APPROPRIATION,
        other_budgetary_resources_amount_cpe=0.1,
        gross_outlay_amount_by_tas_cpe=LATE_GTAS_OUTLAY,
    )


@pytest.fixture
def quarterly_gtas(defc_codes):
    _partial_quarterly_schedule_for_year(EARLY_YEAR)
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        total_budgetary_resources_cpe=QUARTERLY_GTAS_BUDGETARY_RESOURCES,
        budget_authority_appropriation_amount_cpe=0.25,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.02,
    )


@pytest.fixture
def early_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        total_budgetary_resources_cpe=EARLY_GTAS_BUDGETARY_RESOURCES,
        budget_authority_appropriation_amount_cpe=0.19,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.02,
    )


@pytest.fixture
def non_covid_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="A",
        total_budgetary_resources_cpe=0.32,
        budget_authority_appropriation_amount_cpe=0.31,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=0.13,
    )


@pytest.fixture
def unobligated_balance_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        unobligated_balance_cpe=UNOBLIGATED_GTAS_BUDGETARY_RESOURCES,
        disaster_emergency_fund_code="A",
        total_budgetary_resources_cpe=1.5,
        budget_authority_appropriation_amount_cpe=0.74,
        other_budgetary_resources_amount_cpe=0.74,
        gross_outlay_amount_by_tas_cpe=0.0,
    )


@pytest.fixture
def other_budget_authority_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=0,
        disaster_emergency_fund_code="M",
        total_budgetary_resources_cpe=0.85,
        budget_authority_appropriation_amount_cpe=0.69,
        other_budgetary_resources_amount_cpe=0.14,
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
        fiscal_year=LATE_YEAR,
        fiscal_period=EARLY_MONTH,
        unobligated_balance_cpe=YEAR_TWO_GTAS_UNOBLIGATED_BALANCE,
        disaster_emergency_fund_code=code,
        total_budgetary_resources_cpe=YEAR_TWO_GTAS_BUDGETARY_RESOURCES,
        budget_authority_appropriation_amount_cpe=YEAR_TWO_GTAS_APPROPRIATION,
        other_budgetary_resources_amount_cpe=0.0,
        gross_outlay_amount_by_tas_cpe=YEAR_TWO_OUTLAY,
    )


@pytest.fixture
def basic_faba(defc_codes):
    submission = mommy.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=EARLY_YEAR, reporting_fiscal_period=EARLY_MONTH
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
    _year_2_late_faba_with_value(0.7)


@pytest.fixture
def multi_period_faba(defc_codes):
    _year_2_late_faba_with_value(1.6)
    _year_2_faba_with_value(0.7)


@pytest.fixture
def multi_period_faba_with_future(defc_codes):
    _year_2_late_faba_with_value(0.7)
    _year_3_faba_with_value(2.2)


def _year_1_faba(value, code):
    submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=EARLY_YEAR,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start=date(EARLY_YEAR, LATE_MONTH, 1),
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
        reporting_fiscal_year=LATE_YEAR,
        reporting_fiscal_period=EARLY_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start=date(LATE_YEAR, EARLY_MONTH, 1),
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
        reporting_fiscal_year=LATE_YEAR,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start=date(LATE_YEAR, LATE_MONTH, 1),
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0,
        submission=submission,
    )


def _year_3_faba_with_value(value):
    submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=LATE_YEAR + 1,
        reporting_fiscal_period=EARLY_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start=date(LATE_YEAR + 1, EARLY_MONTH, 1),
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0,
        submission=submission,
    )


def _fy_2021_schedule():
    _full_schedule_for_year(EARLY_YEAR)


def _fy_2022_schedule():
    _full_schedule_for_year(LATE_YEAR)


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
