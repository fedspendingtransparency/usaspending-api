import pytest

from decimal import Decimal
from model_bakery import baker
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/budgetary_resources/{filter}"
FY2023 = 2023
FY2022 = 2022
FY2021 = 2021
FY2020 = 2020


@pytest.fixture
def before_2022_data_fixture():
    _build_dabs_reporting_data(FY2021)


@pytest.fixture
def after_2022_data_fixture():
    _build_dabs_reporting_data(FY2022)


def _build_dabs_reporting_data(fy_reported):
    fy_before_fy_reported = fy_reported - 1
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=1995,
        submission_fiscal_month=2,
        submission_fiscal_quarter=1,
        submission_reveal_date="2020-10-09",
    )
    ta1 = baker.make("references.ToptierAgency", toptier_code="001", _fill_optional=True)
    ta2 = baker.make("references.ToptierAgency", toptier_code="002", _fill_optional=True)
    baker.make("references.Agency", toptier_flag=True, toptier_agency=ta1, _fill_optional=True)
    baker.make("references.Agency", toptier_flag=True, toptier_agency=ta2, _fill_optional=True)
    tas1 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    sa1_3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_reported,
        reporting_fiscal_period=3,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_6 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_reported,
        reporting_fiscal_period=6,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_7 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_reported,
        reporting_fiscal_period=7,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_9 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_reported,
        reporting_fiscal_period=9,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=False,
    )
    sa1_12 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_reported,
        reporting_fiscal_period=12,
        submission_window=dabs,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
    )
    sa2_12 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=fy_before_fy_reported,
        reporting_fiscal_period=12,
        submission_window_id=dabs.id,
        toptier_code=ta2.toptier_code,
        is_final_balances_for_fy=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9993,
        obligations_incurred_total_by_tas_cpe=8883,
        treasury_account_identifier=tas1,
        submission=sa1_3,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9996,
        obligations_incurred_total_by_tas_cpe=8886,
        treasury_account_identifier=tas1,
        submission=sa1_6,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9997,
        obligations_incurred_total_by_tas_cpe=8887,
        treasury_account_identifier=tas1,
        submission=sa1_7,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9999,
        obligations_incurred_total_by_tas_cpe=8889,
        treasury_account_identifier=tas1,
        submission=sa1_9,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=1,
        obligations_incurred_total_by_tas_cpe=1,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=3,
        obligations_incurred_total_by_tas_cpe=2,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=15,
        obligations_incurred_total_by_tas_cpe=5,
        treasury_account_identifier=tas1,
        submission=sa2_12,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=31,
        obligations_incurred_total_by_tas_cpe=6,
        treasury_account_identifier=tas2,
        submission=sa1_12,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=63,
        obligations_incurred_total_by_tas_cpe=7,
        treasury_account_identifier=tas2,
        submission=sa2_12,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8883,
        treasury_account=tas1,
        submission=sa1_3,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8886,
        treasury_account=tas1,
        submission=sa1_6,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8887,
        treasury_account=tas1,
        submission=sa1_7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=8889,
        treasury_account=tas1,
        submission=sa1_9,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=14,
        treasury_account=tas1,
        submission=sa1_12,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=2,
        gross_outlay_amount_by_program_object_class_cpe=13,
        treasury_account=tas1,
        submission=sa1_12,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=5,
        gross_outlay_amount_by_program_object_class_cpe=9,
        treasury_account=tas1,
        submission=sa2_12,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=6,
        treasury_account=tas2,
        submission=sa1_12,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        obligations_incurred_by_program_object_class_cpe=7,
        treasury_account=tas2,
        submission=sa2_12,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    for fy in range(2017, current_fiscal_year() + 1):
        baker.make("references.GTASSF133Balances", fiscal_year=fy, fiscal_period=12, total_budgetary_resources_cpe=fy)
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            submission_fiscal_year=fy,
            submission_fiscal_month=12,
            submission_fiscal_quarter=4,
            submission_reveal_date="2020-10-09",
            is_quarter=True,
        )


@pytest.mark.django_db
def test_budgetary_resources_before_2022(client, before_2022_data_fixture):
    """Test accumulation of agency DABS reporting data (budgetary resources); given it is using data ONLY BEFORE
    FY 2022 (i.e. the date 2021-09-30 and before) it SHOULD NOT accumulate monthly period reporting data."""
    resp = client.get(URL.format(code="001", filter=""))
    expected_results = [
        {
            "fiscal_year": FY2021,
            "agency_budgetary_resources": Decimal("4.00"),
            "total_budgetary_resources": Decimal(f"{FY2021}.00"),
            "agency_total_obligated": Decimal("3.00"),
            "agency_total_outlayed": Decimal("27.00"),
            "agency_obligation_by_period": [
                {"obligated": Decimal("8883.00"), "period": 3},
                {"obligated": Decimal("8886.00"), "period": 6},
                {"obligated": Decimal("8889.00"), "period": 9},
                {"obligated": Decimal("3.00"), "period": 12},
            ],
        },
        {
            "fiscal_year": FY2020,
            "agency_budgetary_resources": Decimal("15.00"),
            "total_budgetary_resources": Decimal(f"{FY2020}.00"),
            "agency_total_obligated": Decimal("5.00"),
            "agency_total_outlayed": Decimal("9.00"),
            "agency_obligation_by_period": [{"period": 12, "obligated": Decimal("5.00")}],
        },
    ]
    for year in range(2017, current_fiscal_year() + 1):
        if year != FY2021 and year != FY2020:
            expected_results.append(
                {
                    "fiscal_year": year,
                    "agency_budgetary_resources": None,
                    "total_budgetary_resources": Decimal(f"{year}.00"),
                    "agency_total_outlayed": None,
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


@pytest.mark.django_db
def test_budgetary_resources_after_2022(client, after_2022_data_fixture):
    """Test accumulation of agency DABS reporting data (budgetary resources); given it is using data AFTER FY 2022
    (i.e. the date 2021-10-01 and after) it SHOULD ALSO accumulate monthly period reporting data."""
    resp = client.get(URL.format(code="001", filter=""))
    expected_results = [
        {
            "fiscal_year": FY2022,
            "agency_budgetary_resources": Decimal("4.00"),
            "total_budgetary_resources": Decimal(f"{FY2022}.00"),
            "agency_total_obligated": Decimal("3.00"),
            "agency_total_outlayed": Decimal("27.00"),
            "agency_obligation_by_period": [
                {"obligated": Decimal("8883.00"), "period": 3},
                {"obligated": Decimal("8886.00"), "period": 6},
                # THIS is the difference. Monthly reporting period data are collected AFTER FY 2022, not just quarterly
                {"obligated": Decimal("8887.00"), "period": 7},
                {"obligated": Decimal("8889.00"), "period": 9},
                {"obligated": Decimal("3.00"), "period": 12},
            ],
        },
        {
            "fiscal_year": FY2021,
            "agency_budgetary_resources": Decimal("15.00"),
            "total_budgetary_resources": Decimal(f"{FY2021}.00"),
            "agency_total_obligated": Decimal("5.00"),
            "agency_total_outlayed": Decimal("9.00"),
            "agency_obligation_by_period": [{"period": 12, "obligated": Decimal("5.00")}],
        },
    ]
    for year in range(2017, current_fiscal_year() + 1):
        if year != FY2022 and year != FY2021:
            expected_results.append(
                {
                    "fiscal_year": year,
                    "agency_budgetary_resources": None,
                    "total_budgetary_resources": Decimal(f"{year}.00"),
                    "agency_total_outlayed": None,
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
