import pytest
from django.db.models import Sum
from model_bakery import baker

from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.fixture
def non_zero_test_data():
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=300,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        ussgl480110_rein_undel_ord_cpe=-100,
        ussgl490110_rein_deliv_ord_cpe=-100,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=100,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-100,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=200,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-100,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-100,
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
        prior_year_adjustment="P",
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=100,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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
        prior_year_adjustment="X",
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
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


@pytest.fixture
def obligation_and_outlay_data():
    oc = baker.make("references.ObjectClass")
    is_not_final_sub = baker.make("submissions.SubmissionAttributes", is_final_balances_for_fy=False)
    is_final_sub = baker.make("submissions.SubmissionAttributes", is_final_balances_for_fy=True)
    pya_list = ["B", "B", "P", "P", "X", "X"]
    submission_list = [is_not_final_sub, is_final_sub]
    for pya, sub in zip(pya_list, submission_list * 3):
        baker.make(
            "financial_activities.FinancialAccountsByProgramActivityObjectClass",
            submission=sub,
            object_class=oc,
            prior_year_adjustment=pya,
            obligations_incurred_by_program_object_class_cpe=100,
            gross_outlay_amount_by_program_object_class_cpe=100,
            deobligations_recoveries_refund_pri_program_object_class_cpe=100,
            ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=100,
            ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100,
            ussgl480100_undelivered_orders_obligations_unpaid_cpe=1000,
            ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=1000,
            ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=1000,
            ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=1000,
            ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=1000,
            ussgl490100_delivered_orders_obligations_unpaid_cpe=1000,
            ussgl490200_delivered_orders_obligations_paid_cpe=1000,
            ussgl490800_authority_outlayed_not_yet_disbursed_cpe=1000,
            ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=1000,
            ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=1000,
            ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=1000,
            ussgl480110_rein_undel_ord_cpe=10000,
            ussgl490110_rein_deliv_ord_cpe=10000,
        )


@pytest.mark.django_db
def test_is_non_zero_total_spending_filter(non_zero_test_data):
    covid_calc = FileBCalculations(is_covid_page=True)
    nonzero_count = FinancialAccountsByProgramActivityObjectClass.objects.filter(
        covid_calc.is_non_zero_total_spending()
    ).count()
    assert nonzero_count == 5

    single_year_calc = FileBCalculations(is_covid_page=False)
    nonzero_count = FinancialAccountsByProgramActivityObjectClass.objects.filter(
        single_year_calc.is_non_zero_total_spending()
    ).count()
    assert nonzero_count == 6


@pytest.mark.django_db
def test_get_obligations(obligation_and_outlay_data):
    covid_calc = FileBCalculations(include_final_sub_filter=False, is_covid_page=True)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(covid_calc.get_obligations()))
        .get()
    )
    assert result["total"] == 85200

    covid_final_sub_calc = FileBCalculations(include_final_sub_filter=True, is_covid_page=True)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(covid_final_sub_calc.get_obligations()))
        .get()
    )
    assert result["total"] == 42600

    single_year_calc = FileBCalculations(include_final_sub_filter=False, is_covid_page=False)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(single_year_calc.get_obligations()))
        .get()
    )
    assert result["total"] == 200

    single_year_final_sub_calc = FileBCalculations(include_final_sub_filter=True, is_covid_page=False)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(single_year_final_sub_calc.get_obligations()))
        .get()
    )
    assert result["total"] == 100


@pytest.mark.django_db
def test_get_outlays(obligation_and_outlay_data):
    covid_calc = FileBCalculations(include_final_sub_filter=False, is_covid_page=True)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(covid_calc.get_outlays()))
        .get()
    )
    assert result["total"] == 600

    covid_final_sub_calc = FileBCalculations(include_final_sub_filter=True, is_covid_page=True)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(covid_final_sub_calc.get_outlays()))
        .get()
    )
    assert result["total"] == 300

    single_year_calc = FileBCalculations(include_final_sub_filter=False, is_covid_page=False)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(single_year_calc.get_outlays()))
        .get()
    )
    assert result["total"] == 200

    single_year_final_sub_calc = FileBCalculations(include_final_sub_filter=True, is_covid_page=False)
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(single_year_final_sub_calc.get_outlays()))
        .get()
    )
    assert result["total"] == 100
