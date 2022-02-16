import pytest

from decimal import Decimal
from model_mommy import mommy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/budgetary_resources/{filter}"
FY = current_fiscal_year() - 2
PRIOR_FY = FY - 1


@pytest.fixture
def data_fixture():
    dabs = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=1995,
        submission_fiscal_month=2,
        submission_fiscal_quarter=1,
        submission_reveal_date="2020-10-09",
    )
    ta1 = mommy.make("references.ToptierAgency", toptier_code="001")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002")
    mommy.make("references.Agency", toptier_flag=True, toptier_agency=ta1)
    mommy.make("references.Agency", toptier_flag=True, toptier_agency=ta2)
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    sa1_3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=FY,
        reporting_fiscal_period=3,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_6 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=FY,
        reporting_fiscal_period=6,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_7 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=FY,
        reporting_fiscal_period=7,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_9 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=FY,
        reporting_fiscal_period=9,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_12 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=FY,
        reporting_fiscal_period=12,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
    )
    sa2_12 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=PRIOR_FY,
        reporting_fiscal_period=12,
        submission_window_id=dabs.id,
        toptier_code=ta2.toptier_code,
        is_final_balances_for_fy=True,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9993,
        obligations_incurred_total_by_tas_cpe=8883,
        treasury_account_identifier=tas1,
        submission=sa1_3,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9996,
        obligations_incurred_total_by_tas_cpe=8886,
        treasury_account_identifier=tas1,
        submission=sa1_6,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9997,
        obligations_incurred_total_by_tas_cpe=8887,
        treasury_account_identifier=tas1,
        submission=sa1_7,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9999,
        obligations_incurred_total_by_tas_cpe=8889,
        treasury_account_identifier=tas1,
        submission=sa1_9,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=1,
        obligations_incurred_total_by_tas_cpe=1,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=3,
        obligations_incurred_total_by_tas_cpe=2,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=15,
        obligations_incurred_total_by_tas_cpe=5,
        treasury_account_identifier=tas1,
        submission=sa2_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=31,
        obligations_incurred_total_by_tas_cpe=6,
        treasury_account_identifier=tas2,
        submission=sa1_12,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=63,
        obligations_incurred_total_by_tas_cpe=7,
        treasury_account_identifier=tas2,
        submission=sa2_12,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8883,
        treasury_account=tas1,
        submission=sa1_3,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8886,
        treasury_account=tas1,
        submission=sa1_6,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8887,
        treasury_account=tas1,
        submission=sa1_7,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8889,
        treasury_account=tas1,
        submission=sa1_9,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=1,
        treasury_account=tas1,
        submission=sa1_12,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=2,
        treasury_account=tas1,
        submission=sa1_12,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=5,
        treasury_account=tas1,
        submission=sa2_12,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=6,
        treasury_account=tas2,
        submission=sa1_12,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=7,
        treasury_account=tas2,
        submission=sa2_12,
    )

    for fy in range(2017, current_fiscal_year() + 1):
        mommy.make("references.GTASSF133Balances", fiscal_year=fy, fiscal_period=12, total_budgetary_resources_cpe=fy)
        mommy.make(
            "submissions.DABSSubmissionWindowSchedule",
            submission_fiscal_year=fy,
            submission_fiscal_month=12,
            submission_fiscal_quarter=4,
            submission_reveal_date="2020-10-09",
            is_quarter=True,
        )


@pytest.mark.django_db
def test_budgetary_resources(client, data_fixture):
    resp = client.get(URL.format(code="001", filter=""))
    expected_results = [
        {
            "fiscal_year": FY,
            "agency_budgetary_resources": Decimal("4.00"),
            "total_budgetary_resources": Decimal(f"{FY}.00"),
            "agency_total_obligated": Decimal("3.00"),
            "agency_obligation_by_period": [
                {"obligated": Decimal("8883.00"), "period": 3},
                {"obligated": Decimal("8886.00"), "period": 6},
                {"obligated": Decimal("8889.00"), "period": 9},
                {"obligated": Decimal("3.00"), "period": 12},
            ],
        },
        {
            "fiscal_year": PRIOR_FY,
            "agency_budgetary_resources": Decimal("15.00"),
            "total_budgetary_resources": Decimal(f"{PRIOR_FY}.00"),
            "agency_total_obligated": Decimal("5.00"),
            "agency_obligation_by_period": [{"period": 12, "obligated": Decimal("5.00")}],
        },
    ]
    for year in range(2017, current_fiscal_year() + 1):
        if year != FY and year != PRIOR_FY:
            expected_results.append(
                {
                    "fiscal_year": year,
                    "agency_budgetary_resources": None,
                    "total_budgetary_resources": Decimal(f"{year}.00"),
                    "agency_total_obligated": None,
                    "agency_obligation_by_period": [],
                }
            )
    expected_results = sorted(expected_results, key=lambda x: x["fiscal_year"], reverse=True)
    assert resp.data == {
        "toptier_code": "001",
        "agency_data_by_year": expected_results,
        "messages": [],
    }
