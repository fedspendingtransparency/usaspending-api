import pytest
from rest_framework import status
from model_bakery import baker
from .conftest import CURRENT_FISCAL_YEAR

url = "/api/v2/agency/treasury_account/{tas}/program_activity/{query_params}"


@pytest.mark.django_db
def test_tas_program_activity_success(client, monkeypatch, agency_account_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
        },
        "results": [
            {
                "name": "NAME 3",
                "gross_outlay_amount": 100000.0,
                "obligated_amount": 100.0,
                "children": [
                    {
                        "name": "Other",
                        "gross_outlay_amount": 100000.0,
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "name": "NAME 2",
                "gross_outlay_amount": 1000000.0,
                "obligated_amount": 10.0,
                "children": [
                    {
                        "name": "Other",
                        "gross_outlay_amount": 1000000.0,
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "name": "NAME 1",
                "gross_outlay_amount": 10000000.0,
                "obligated_amount": 1.0,
                "children": [
                    {
                        "name": "Other",
                        "gross_outlay_amount": 10000000.0,
                        "obligated_amount": 1.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # Tests non-existent tas
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-999"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "002-2008/2009-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 5",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "name": "Other",
                        "gross_outlay_amount": 1000000.0,
                        "obligated_amount": 10.0,
                    }
                ],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_multiple_program_activity_belonging_one_object_class(
    client, monkeypatch, tas_mulitple_pas_per_oc, helpers
):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 2,
        },
        "results": [
            {
                "name": "NAME 2",
                "gross_outlay_amount": 1000000.0,
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "Other",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "name": "NAME 1",
                "gross_outlay_amount": 10000000.0,
                "obligated_amount": 1.0,
                "children": [
                    {
                        "gross_outlay_amount": 10000000.0,
                        "name": "Other",
                        "obligated_amount": 1.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_program_activity_multiple_submission_years(client, agency_account_data):
    tas = "002-X-0000-000"
    submission_year = 2017
    query_params = f"?fiscal_year={submission_year}"
    resp = client.get(url.format(tas=tas, query_params=query_params))
    expected_result = {
        "fiscal_year": submission_year,
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 10000.0,
                "name": "NAME 4",
                "obligated_amount": 1000.0,
                "children": [{"gross_outlay_amount": 10000.0, "name": "Other", "obligated_amount": 1000.0}],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_program_activity_multiple_object_classes(client, tas_mulitple_oc_per_tas):
    tas = "002-X-0000-000"
    submission_year = 2020
    query_params = f"?fiscal_year={submission_year}"
    resp = client.get(url.format(tas=tas, query_params=query_params))
    expected_result = {
        "fiscal_year": submission_year,
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "name": "NAME 4",
                "gross_outlay_amount": 111000.0,
                "obligated_amount": 11100.0,
                "children": [
                    {
                        "gross_outlay_amount": 101000.0,
                        "name": "Other",
                        "obligated_amount": 10100.0,
                    },
                    {
                        "gross_outlay_amount": 10000.0,
                        "name": "Other2",
                        "obligated_amount": 1000.0,
                    },
                ],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_with_no_program_activity(client, monkeypatch, tas_with_no_object_class, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.fixture
def tas_mulitple_oc_per_tas():
    """Sets up test data such that for a specific treasury account there are multiple
    object classes associated with that treasury account and one program activity.
    """
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = baker.make("references.ToptierAgency", toptier_code="008", _fill_optional=True)

    baker.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, _fill_optional=True)

    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    fa1 = baker.make("accounts.FederalAccount", federal_account_code="002-0000", account_title="FA 2")

    tas1 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=200,
        budget_function_title="NAME 2",
        budget_subfunction_code=2100,
        budget_subfunction_title="NAME 2A",
        federal_account=fa1,
        account_title="TA 2",
        tas_rendering_label="002-X-0000-000",
    )

    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)

    pa1 = baker.make("references.RefProgramActivity", program_activity_code="111", program_activity_name="NAME 4")

    oc = "references.ObjectClass"
    oc1 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=120, object_class_name="supplies"
    )
    oc2 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other2", object_class=130, object_class_name="interest"
    )
    oc3 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=120, object_class_name="supplies"
    )

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc2,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=10000,
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
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=1000,
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
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
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
