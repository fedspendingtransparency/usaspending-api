import pytest

from datetime import date
from decimal import Decimal
from model_bakery import baker
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.disaster.v2.views.disaster_base import COVID_19_GROUP_NAME


def to_decimal(input):
    return Decimal(f"{round(input, 2)}")


def calculate_values(input_dict):
    TOTAL_BUDGETARY_RESOURCES = input_dict["total_budgetary_resources_cpe"] - (
        input_dict["budget_authority_unobligated_balance_brought_forward_cpe"]
        + input_dict["deobligations_or_recoveries_or_refunds_from_prior_year_cpe"]
        + input_dict["prior_year_paid_obligation_recoveries"]
    )
    TOTAL_OBLIGATIONS = input_dict["obligations_incurred_total_cpe"] - (
        input_dict["deobligations_or_recoveries_or_refunds_from_prior_year_cpe"]
    )
    TOTAL_OUTLAYS = (
        input_dict["gross_outlay_amount_by_tas_cpe"] - input_dict["anticipated_prior_year_obligation_recoveries"]
    )

    return {
        "total_budgetary_resources": to_decimal(TOTAL_BUDGETARY_RESOURCES),
        "total_obligations": to_decimal(TOTAL_OBLIGATIONS),
        "total_outlays": to_decimal(TOTAL_OUTLAYS),
    }


NOT_COVID_NAME = "not_covid_19"

EARLY_MONTH = 3
LATE_MONTH = 12

EARLY_YEAR = 2021
LATE_YEAR = 2022


LATE_GTAS = {
    "total_budgetary_resources_cpe": 0.30,
    "budget_authority_unobligated_balance_brought_forward_cpe": 0.0,
    "gross_outlay_amount_by_tas_cpe": 0.03,
    "obligations_incurred_total_cpe": 4.0,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 0.0,
    "prior_year_paid_obligation_recoveries": 0.0,
    "anticipated_prior_year_obligation_recoveries": 0.0,
}
LATE_GTAS_CALCULATIONS = calculate_values(LATE_GTAS)

QUARTERLY_GTAS = {
    "total_budgetary_resources_cpe": 0.26,
    "budget_authority_unobligated_balance_brought_forward_cpe": 1.0,
    "gross_outlay_amount_by_tas_cpe": 0.07,
    "obligations_incurred_total_cpe": 0.08,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 0.5,
    "prior_year_paid_obligation_recoveries": 0.3,
    "anticipated_prior_year_obligation_recoveries": 0.2,
}
QUARTERLY_GTAS_CALCULATIONS = calculate_values(QUARTERLY_GTAS)

EARLY_GTAS = {
    "total_budgetary_resources_cpe": 0.20,
    "budget_authority_unobligated_balance_brought_forward_cpe": 0.05,
    "gross_outlay_amount_by_tas_cpe": 0.02,
    "obligations_incurred_total_cpe": 4.0,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 0.0,
    "prior_year_paid_obligation_recoveries": 0.0,
    "anticipated_prior_year_obligation_recoveries": 0.0,
}
EARLY_GTAS_CALCULATIONS = calculate_values(EARLY_GTAS)

YEAR_2_GTAS = {
    "total_budgetary_resources_cpe": 0.32,
    "budget_authority_unobligated_balance_brought_forward_cpe": 0.0,
    "gross_outlay_amount_by_tas_cpe": 0.07,
    "obligations_incurred_total_cpe": 3.0,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 0.3,
    "prior_year_paid_obligation_recoveries": 0.1,
    "anticipated_prior_year_obligation_recoveries": 0.1,
}
YEAR_2_GTAS_CALCULATIONS = calculate_values(YEAR_2_GTAS)

UNOBLIGATED_BALANCE_GTAS = {
    "total_budgetary_resources_cpe": 1.5,
    "budget_authority_unobligated_balance_brought_forward_cpe": 0.0,
    "gross_outlay_amount_by_tas_cpe": 0.0,
    "obligations_incurred_total_cpe": 4.0,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 4.0,
    "prior_year_paid_obligation_recoveries": 0.0,
    "anticipated_prior_year_obligation_recoveries": 0.0,
}
UNOBLIGATED_BALANCE_GTAS_CALCULATIONS = calculate_values(UNOBLIGATED_BALANCE_GTAS)

OTHER_BUDGET_AUTHORITY_GTAS = {
    "total_budgetary_resources_cpe": 0.85,
    "budget_authority_unobligated_balance_brought_forward_cpe": 0.0,
    "gross_outlay_amount_by_tas_cpe": 0.2,
    "obligations_incurred_total_cpe": 4.0,
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 4.0,
    "prior_year_paid_obligation_recoveries": 0.0,
    "anticipated_prior_year_obligation_recoveries": 0.0,
}
OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS = calculate_values(OTHER_BUDGET_AUTHORITY_GTAS)


@pytest.fixture
def defc_codes():
    return [
        baker.make("references.DisasterEmergencyFundCode", code="A", group_name=NOT_COVID_NAME),
        baker.make("references.DisasterEmergencyFundCode", code="M", group_name=COVID_19_GROUP_NAME),
        baker.make("references.DisasterEmergencyFundCode", code="N", group_name=COVID_19_GROUP_NAME),
        baker.make("references.DisasterEmergencyFundCode", code="O", group_name=COVID_19_GROUP_NAME),
        baker.make("references.DisasterEmergencyFundCode", code="V", group_name=COVID_19_GROUP_NAME),
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
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        disaster_emergency_fund_id="M",
        **LATE_GTAS,
    )


@pytest.fixture
def quarterly_gtas(defc_codes):
    _partial_quarterly_schedule_for_year(EARLY_YEAR)
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        disaster_emergency_fund_id="M",
        **QUARTERLY_GTAS,
    )


@pytest.fixture
def early_gtas(defc_codes):
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=EARLY_MONTH,
        disaster_emergency_fund_id="M",
        **EARLY_GTAS,
    )


@pytest.fixture
def unobligated_balance_gtas(defc_codes):
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=LATE_MONTH,
        disaster_emergency_fund_id="A",
        **UNOBLIGATED_BALANCE_GTAS,
    )


@pytest.fixture
def other_budget_authority_gtas(defc_codes):
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=EARLY_YEAR,
        fiscal_period=EARLY_MONTH,
        disaster_emergency_fund_id="M",
        **OTHER_BUDGET_AUTHORITY_GTAS,
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
    baker.make(
        "references.GTASSF133Balances",
        fiscal_year=LATE_YEAR,
        fiscal_period=EARLY_MONTH,
        disaster_emergency_fund_id=code,
        **YEAR_2_GTAS,
    )


@pytest.fixture
def basic_faba(defc_codes):
    submission = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=EARLY_YEAR, reporting_fiscal_period=EARLY_MONTH
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.all().first(),
        transaction_obligated_amount=0.0 - 0.3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.1,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0.2,
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
    submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=EARLY_YEAR,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start=date(EARLY_YEAR, LATE_MONTH, 1),
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code=code).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0 - 0.3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.1,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0.2,
        submission=submission,
    )


def _year_2_faba_with_value(value):
    submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=LATE_YEAR,
        reporting_fiscal_period=EARLY_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start=date(LATE_YEAR, EARLY_MONTH, 1),
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0 - 0.7,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.4,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0.3,
        submission=submission,
    )


def _year_2_late_faba_with_value(value):
    submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=LATE_YEAR,
        reporting_fiscal_period=LATE_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start=date(LATE_YEAR, LATE_MONTH, 1),
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0 - 0.2,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.1,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0.1,
        submission=submission,
    )


def _year_3_faba_with_value(value):
    submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=LATE_YEAR + 1,
        reporting_fiscal_period=EARLY_MONTH,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start=date(LATE_YEAR + 1, EARLY_MONTH, 1),
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        transaction_obligated_amount=value,
        gross_outlay_amount_by_award_cpe=value / 2.0 - 15.3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=7.1,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=8.2,
        submission=submission,
    )


def _fy_2021_schedule():
    _full_schedule_for_year(EARLY_YEAR)


def _fy_2022_schedule():
    _full_schedule_for_year(LATE_YEAR)


def _full_schedule_for_year(year):
    for month in range(1, LATE_MONTH + 1):
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=False,
            submission_fiscal_year=year,
            submission_fiscal_quarter=(month / 3) + 1,
            submission_fiscal_month=month,
            submission_reveal_date=f"{year}-{month}-15",
        )


def _partial_quarterly_schedule_for_year(year):
    for quarter in range(1, 4):
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=True,
            submission_fiscal_year=year,
            submission_fiscal_quarter=quarter,
            submission_fiscal_month=quarter * 3,
            submission_reveal_date=f"{year}-{quarter * 3}-15",
        )


def _incomplete_schedule_for_year(year):
    for month in range(1, EARLY_MONTH + 1):
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=False,
            submission_fiscal_year=year,
            submission_fiscal_quarter=(month / 3) + 1,
            submission_fiscal_month=month,
            submission_reveal_date=f"{year}-{month}-15",
        )
