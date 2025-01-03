import pytest

from django.db.models import Sum
from model_bakery import baker

from usaspending_api.common.calculations.file_b import is_non_zero_total_spending, get_obligations, get_outlays
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
        ussgl480110_reinstated_del_cpe=-100,
        ussgl490110_reinstated_del_cpe=-100,
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
        ussgl480110_reinstated_del_cpe=0,
        ussgl490110_reinstated_del_cpe=0,
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
            ussgl480110_reinstated_del_cpe=10000,
            ussgl490110_reinstated_del_cpe=10000,
        )


@pytest.mark.django_db
def test_is_non_zero_total_spending_filter(non_zero_test_data):
    """
    This should return a total of three File B records for each of the following cases:
        - One record includes PYA == "P" and wouldn't have the columns necessary for the non-zero check
        - One record includes Obligation total of 100 while Outlay is 0
        - One record includes Outlay total of 100 while Obligation is 0
    """
    nonzero_count = FinancialAccountsByProgramActivityObjectClass.objects.filter(is_non_zero_total_spending()).count()
    assert nonzero_count == 3


@pytest.mark.django_db
def test_get_obligations(obligation_and_outlay_data):
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_obligations(is_multi_year=True, include_final_sub_filter=False)))
        .get()
    )
    assert result["total"] == 125200

    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_obligations(is_multi_year=True, include_final_sub_filter=True)))
        .get()
    )
    assert result["total"] == 62600

    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_obligations(is_multi_year=False, include_final_sub_filter=False)))
        .get()
    )
    assert result["total"] == 40400

    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_obligations(is_multi_year=False, include_final_sub_filter=True)))
        .get()
    )
    assert result["total"] == 20200


@pytest.mark.django_db
def test_get_outlays(obligation_and_outlay_data):
    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_outlays(include_final_sub_filter=False)))
        .get()
    )
    assert result["total"] == 600

    result = (
        FinancialAccountsByProgramActivityObjectClass.objects.values("object_class")
        .annotate(total=Sum(get_outlays(include_final_sub_filter=True)))
        .get()
    )
    assert result["total"] == 300
