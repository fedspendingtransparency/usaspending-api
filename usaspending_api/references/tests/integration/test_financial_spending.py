import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):
    # Create 2 objects that should be returned and one that should not.
    # Create AGENCY AND TopTier AGENCY For FinancialAccountsByProgramActivityObjectClass objects
    ttagency1 = baker.make("references.ToptierAgency", name="tta_name", toptier_code="abc", _fill_optional=True)
    baker.make("references.Agency", id=1, toptier_agency=ttagency1, _fill_optional=True)

    # Object 1
    tas1 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    # Financial Account with Object class and submission
    object_class_1 = baker.make(
        "references.ObjectClass",
        major_object_class="10",
        major_object_class_name="mocName",
        object_class="ocCode",
        object_class_name="ocName",
    )
    submission_1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        toptier_code="abc",
        is_final_balances_for_fy=True,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_1,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_1,
        treasury_account=tas1,
        gross_outlay_amount_by_program_object_class_cpe=0,
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

    # Object 2 (contains 2 fabpaoc s)
    object_class_2 = baker.make(
        "references.ObjectClass",
        major_object_class="10",
        major_object_class_name="mocName2",
        object_class="ocCode2",
        object_class_name="ocName2",
    )
    submission_2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        toptier_code="abc",
        is_final_balances_for_fy=True,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_2,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_2,
        treasury_account=tas1,
        gross_outlay_amount_by_program_object_class_cpe=0,
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
        object_class=object_class_2,
        obligations_incurred_by_program_object_class_cpe=2000,
        submission=submission_2,
        treasury_account=tas1,
        gross_outlay_amount_by_program_object_class_cpe=0,
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

    # 2018, not reported by 2017 api call
    object_class_0 = baker.make(
        "references.ObjectClass",
        major_object_class="00",
        major_object_class_name="Zero object type, override me",
        object_class="ocCode2",
        object_class_name="ocName2",
    )
    tas3 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    submission_3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2018,
        toptier_code="abc",
        is_final_balances_for_fy=True,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_0,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_3,
        treasury_account=tas3,
        gross_outlay_amount_by_program_object_class_cpe=0,
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


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get("/api/v2/financial_spending/major_object_class/?fiscal_year=2017&funding_agency_id=1")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # make sure resp.data['results'] contains a 3000 value
    assert (
        resp.data["results"][1]["obligated_amount"] == "3000.00"
        or resp.data["results"][0]["obligated_amount"] == "3000.00"
    )

    # check for bad request due to missing params
    resp = client.get("/api/v2/financial_spending/object_class/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    resp = client.get(
        "/api/v2/financial_spending/object_class/?fiscal_year=2017&funding_agency_id=1&major_object_class_code=10"
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # make sure resp.data['results'] contains a 3000 value
    assert (
        resp.data["results"][1]["obligated_amount"] == "3000.00"
        or resp.data["results"][0]["obligated_amount"] == "3000.00"
    )

    # check for bad request due to missing params
    resp = client.get("/api/v2/financial_spending/object_class/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_type_endpoint_no_object_class(client, financial_spending_data):
    """Test the award_type endpoint in the major object class 00 special case.

    Object class 00 should be reported as 'Unknown Object Type' despite
    name in database."""

    resp = client.get("/api/v2/financial_spending/major_object_class/?fiscal_year=2018&funding_agency_id=1")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    # verify that object class name has been overridden
    assert resp.data["results"][0]["major_object_class_name"] == "Unknown Object Type"
