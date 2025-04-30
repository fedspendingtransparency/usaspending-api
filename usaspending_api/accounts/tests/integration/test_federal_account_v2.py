import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models.bureau_title_lookup import BureauTitleLookup


@pytest.fixture
def fixture_data(db):
    ta0 = baker.make(
        "references.ToptierAgency", toptier_code="001", abbreviation="ABCD", name="Dept. of Depts", _fill_optional=True
    )
    ta1 = baker.make(
        "references.ToptierAgency", toptier_code="002", abbreviation="EFGH", name="The Bureau", _fill_optional=True
    )
    ta2 = baker.make(
        "references.ToptierAgency",
        toptier_code="1601",
        abbreviation="DOL",
        name="Department of Labor",
        _fill_optional=True,
    )
    ta3 = baker.make(
        "references.ToptierAgency",
        toptier_code="097",
        abbreviation="DOD",
        name="Department of Defense",
        _fill_optional=True,
    )
    ta4 = baker.make(
        "references.ToptierAgency",
        toptier_code="021",
        abbreviation="DOD",
        name="Department of Navy",
        _fill_optional=True,
    )
    fa0 = baker.make(
        FederalAccount,
        agency_identifier="001",
        main_account_code="0005",
        account_title="Something",
        federal_account_code="001-0005",
        parent_toptier_agency=ta0,
    )
    fa1 = baker.make(
        FederalAccount,
        agency_identifier="002",
        main_account_code="0005",
        account_title="Nothing1",
        federal_account_code="002-0005",
        parent_toptier_agency=ta1,
    )
    fa2 = baker.make(
        FederalAccount,
        agency_identifier="1600",
        main_account_code="0005",
        account_title="Nothing2",
        federal_account_code="1600-0005",
        parent_toptier_agency=ta2,
    )
    fa3 = baker.make(
        FederalAccount,
        agency_identifier="097",
        main_account_code="0005",
        account_title="CGAC_DOD",
        federal_account_code="097-0005",
        parent_toptier_agency=ta3,
    )
    fa4 = baker.make(
        FederalAccount,
        agency_identifier="021",
        main_account_code="0005",
        account_title="CGAC_DOD(NAVY)",
        federal_account_code="021-0005",
        parent_toptier_agency=ta4,
    )

    ta0 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa0, tas_rendering_label="tas-label-0")
    ta1 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa1, tas_rendering_label="tas-label-1")
    ta2 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa2, tas_rendering_label="tas-label-2")
    ta3 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa3, tas_rendering_label="tas-label-3")
    ta4 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa4, tas_rendering_label="tas-label-4")

    dabs100 = baker.make(
        "submissions.DABSSubmissionWindowSchedule", submission_reveal_date="2017-11-01", submission_fiscal_year=2017
    )
    sub101 = baker.make(
        "submissions.SubmissionAttributes",
        submission_id="101",
        reporting_fiscal_year=2017,
        is_final_balances_for_fy=True,
        submission_window_id=dabs100.id,
    )
    sub102 = baker.make(
        "submissions.SubmissionAttributes",
        submission_id="102",
        reporting_fiscal_year=2017,
        is_final_balances_for_fy=False,
        submission_window_id=dabs100.id,
    )
    sub103 = baker.make(
        "submissions.SubmissionAttributes",
        submission_id="103",
        reporting_fiscal_year=2017,
        is_final_balances_for_fy=True,
        submission_window_id=dabs100.id,
    )

    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=1000,
        submission=sub101,
    )
    # Will be filtered out because it's submission's "is_final_balances_for_fy" is False
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=100,
        submission__reporting_period_start="2017-03-01",
        submission=sub102,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=2000,
        submission=sub101,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta1,
        total_budgetary_resources_amount_cpe=9000,
        submission=sub103,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta1,
        total_budgetary_resources_amount_cpe=500,
        submission__reporting_period_start="2016-06-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier__treasury_account_identifier="999",
        total_budgetary_resources_amount_cpe=4000,
        submission__reporting_period_start="2017-06-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta2,
        total_budgetary_resources_amount_cpe=1000,
        submission__reporting_period_start="2015-06-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta3,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2018-03-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta4,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2018-03-02",
    )

    ta99 = baker.make(
        "references.ToptierAgency", toptier_code="999", name="Dept. of Depts", abbreviation=None, _fill_optional=True
    )

    fa99 = baker.make(
        FederalAccount,
        id="9999",
        agency_identifier="999",
        main_account_code="0009",
        account_title="Custom 99",
        federal_account_code="999-0009",
        parent_toptier_agency=ta99,
    )

    taa99 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        account_title="Cool Treasury Account",
        federal_account=fa99,
        tas_rendering_label="tas-label-99",
    )

    baker.make(
        BureauTitleLookup,
        federal_account_code="999-0009",
        bureau_title="Test Bureau",
        bureau_slug="test-bureau",
    )

    dabs99 = baker.make(
        "submissions.DABSSubmissionWindowSchedule", submission_reveal_date="2022-09-01", submission_fiscal_year=2022
    )

    sub99 = baker.make(
        "submissions.SubmissionAttributes",
        submission_id="099",
        reporting_fiscal_year=2022,
        is_final_balances_for_fy=True,
        submission_window_id=dabs99.id,
    )
    sub100 = baker.make(
        "submissions.SubmissionAttributes",
        submission_id="100",
        reporting_fiscal_year=2022,
        is_final_balances_for_fy=False,
        submission_window_id=dabs99.id,
    )

    baker.make(
        FinancialAccountsByProgramActivityObjectClass,
        treasury_account=taa99,
        submission=sub99,
        obligations_incurred_by_program_object_class_cpe=500,
        gross_outlay_amount_by_program_object_class_cpe=800,
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
        FinancialAccountsByProgramActivityObjectClass,
        treasury_account=taa99,
        submission=sub100,
        obligations_incurred_by_program_object_class_cpe=501,
        gross_outlay_amount_by_program_object_class_cpe=801,
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
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        submission=sub99,
        treasury_account_identifier=taa99,
        total_budgetary_resources_amount_cpe=1000,
    )

    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        submission=sub100,
        treasury_account_identifier=taa99,
        total_budgetary_resources_amount_cpe=1001,
    )


@pytest.mark.django_db
def test_federal_accounts_endpoint_exists(client, fixture_data):
    """Verify the federal accounts endpoint returns a status of 200"""
    resp = client.post(
        "/api/v2/federal_accounts/", content_type="application/json", data=json.dumps({"filters": {"fy": "2017"}})
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_federal_accounts_endpoint_correct_form(client, fixture_data):
    """Verify the correct keys exist within the response"""
    resp = client.post(
        "/api/v2/federal_accounts/", content_type="application/json", data=json.dumps({"filters": {"fy": "2017"}})
    )
    response_data = resp.json()
    assert response_data["page"] == 1
    assert "limit" in response_data
    assert "count" in response_data
    assert "fy" in response_data
    results = response_data["results"]
    assert "account_number" in results[0]


@pytest.mark.django_db
def test_federal_accounts_endpoint_correct_data(client, fixture_data):
    """Verify federal accounts endpoint returns the correct data"""
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "managing_agency", "direction": "asc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()

    assert response_data["fy"] == "2017"

    assert response_data["results"][0]["managing_agency_acronym"] == "DOD"
    assert response_data["results"][0]["budgetary_resources"] is None

    assert response_data["results"][1]["managing_agency_acronym"] == "DOL"
    assert response_data["results"][1]["budgetary_resources"] is None

    assert response_data["results"][2]["managing_agency_acronym"] == "DOD"
    assert response_data["results"][2]["budgetary_resources"] is None

    assert response_data["results"][3]["managing_agency_acronym"] == "ABCD"
    assert response_data["results"][3]["budgetary_resources"] == 3000

    assert response_data["results"][4]["managing_agency_acronym"] is None
    assert response_data["results"][4]["budgetary_resources"] is None

    assert response_data["results"][5]["managing_agency_acronym"] == "EFGH"
    assert response_data["results"][5]["budgetary_resources"] == 9000


@pytest.mark.django_db
def test_federal_accounts_endpoint_sorting_managing_agency(client, fixture_data):
    """Verify that managing agency sorts are applied correctly"""

    # sort by managing agency, asc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "managing_agency", "direction": "asc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["managing_agency"] < response_data["results"][1]["managing_agency"]

    # sort by managing agency, desc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "managing_agency", "direction": "desc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["managing_agency"] > response_data["results"][1]["managing_agency"]


@pytest.mark.django_db
def test_federal_accounts_endpoint_sorting_account_number(client, fixture_data):
    """Verify that account number sorts are applied correctly"""

    # sort by account number, asc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "account_number", "direction": "asc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["account_number"] < response_data["results"][1]["account_number"]

    # sort by account number, desc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "account_number", "direction": "desc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["account_number"] > response_data["results"][1]["account_number"]


@pytest.mark.django_db
def test_federal_accounts_endpoint_sorting_budgetary_resources(client, fixture_data):
    """Verify that budgetary resources sorts are applied correctly"""

    # sort by budgetary resources, asc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "budgetary_resources", "direction": "asc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["budgetary_resources"] < response_data["results"][1]["budgetary_resources"]

    # sort by budgetary resources, desc
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "budgetary_resources", "direction": "desc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["budgetary_resources"] > response_data["results"][1]["budgetary_resources"]


@pytest.mark.django_db
def test_federal_accounts_endpoint_keyword_filter_account_number(client, fixture_data):
    """Verify that budgetary resources sorts are applied correctly"""

    # filter by the "Bureau" keyword
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"filters": {"fy": "2017"}, "keyword": "001-000"}),
    )
    response_data = resp.json()
    assert len(response_data["results"]) == 1
    assert response_data["results"][0]["account_number"] == "001-0005"


@pytest.mark.django_db
def test_federal_accounts_endpoint_keyword_filter_account_name(client, fixture_data):
    """Verify that budgetary resources sorts are applied correctly"""

    # filter by the "Bureau" keyword
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"filters": {"fy": "2017"}, "keyword": "someth"}),
    )
    response_data = resp.json()
    assert len(response_data["results"]) == 1
    assert response_data["results"][0]["account_name"] == "Something"


@pytest.mark.django_db
def test_federal_accounts_endpoint_keyword_filter_agency_name(client, fixture_data):
    """Verify that budgetary resources sorts are applied correctly"""

    # filter by the "Bureau" keyword
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"filters": {"fy": "2017"}, "keyword": "burea"}),
    )
    response_data = resp.json()
    assert len(response_data["results"]) == 1
    assert response_data["results"][0]["managing_agency"] == "The Bureau"


@pytest.mark.django_db
def test_federal_accounts_endpoint_keyword_filter_agency_acronym(client, fixture_data):
    """Verify that budgetary resources sorts are applied correctly"""

    # filter by the "Bureau" keyword
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"filters": {"fy": "2017"}, "keyword": "efgh"}),
    )
    response_data = resp.json()
    assert len(response_data["results"]) == 1
    assert response_data["results"][0]["managing_agency_acronym"] == "EFGH"


@pytest.mark.django_db
def test_federal_accounts_uses_corrected_cgac(client, fixture_data):
    """Verify that CGAC reported as 1600 in FederalAccount will map to ToptierAgency 1601"""
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "managing_agency", "direction": "asc"}, "filters": {"fy": "2015"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["managing_agency_acronym"] == "DOD"


@pytest.mark.django_db
def test_federal_account_content(client, fixture_data):
    """Verify the correct Federal Account is returned with the correct contents"""
    resp = client.get("/api/v2/federal_accounts/999-0009/", data={"fiscal_year": 2022})

    expected_result = {
        "fiscal_year": "2022",
        "id": 9999,
        "agency_identifier": "999",
        "main_account_code": "0009",
        "account_title": "Custom 99",
        "federal_account_code": "999-0009",
        "parent_agency_toptier_code": "999",
        "parent_agency_name": "Dept. of Depts",
        "bureau_name": "Test Bureau",
        "bureau_slug": "test-bureau",
        "total_obligated_amount": 500.0,
        "total_gross_outlay_amount": 800.0,
        "total_budgetary_resources": 1000.0,
        "children": [
            {
                "name": "Cool Treasury Account",
                "code": "tas-label-99",
                "obligated_amount": 500.0,
                "gross_outlay_amount": 800.0,
                "budgetary_resources_amount": 1000.0,
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_with_no_submissions(client, fixture_data):
    """Verify the Federal Account data is returned even if there aren't any valid submissions"""
    resp = client.get("/api/v2/federal_accounts/999-0009/", data={"fiscal_year": 1776})

    expected_result = {
        "fiscal_year": "1776",
        "id": 9999,
        "agency_identifier": "999",
        "main_account_code": "0009",
        "account_title": "Custom 99",
        "federal_account_code": "999-0009",
        "parent_agency_toptier_code": "999",
        "parent_agency_name": "Dept. of Depts",
        "bureau_name": "Test Bureau",
        "bureau_slug": "test-bureau",
        "total_obligated_amount": None,
        "total_gross_outlay_amount": None,
        "total_budgetary_resources": None,
        "children": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_invalid_param(client, fixture_data):
    """Verify the an invalid federal account code will return as a 400"""
    resp = client.get("/api/v2/federal_accounts/001-0006/")

    assert resp.status_code == 400


@pytest.mark.django_db
def test_federal_account_dod_cgac(client, fixture_data):
    """Verify DOD CGAC query returns CGAC code for all DOD departments in addition to DOD's '097'"""
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"agency_identifier": "097", "filters": {"fy": "2018"}}),
    )
    response_data = resp.json()

    assert len(response_data["results"]) == 6
    assert "Something" in response_data["results"][0]["account_name"]
    assert "Nothing1" in response_data["results"][1]["account_name"]
    assert "CGAC_DOD(NAVY)" in response_data["results"][2]["account_name"]
    assert "CGAC_DOD" in response_data["results"][3]["account_name"]
    assert "Nothing2" in response_data["results"][4]["account_name"]
    assert "Custom 99" in response_data["results"][5]["account_name"]
