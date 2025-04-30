import copy
import json
import pytest

from datetime import datetime, timezone
from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.references.models import Agency, GTASSF133Balances, ToptierAgency, ObjectClass
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule, SubmissionAttributes
from usaspending_api.search.models import TransactionSearch, AwardSearch

ENDPOINT_URL = "/api/v2/spending/"
CONTENT_TYPE = "application/json"
GLOBAL_MOCK_DICT = [
    {
        "model": DABSSubmissionWindowSchedule,
        "id": 1600031,
        "period_end_date": datetime(1599, 12, 31, tzinfo=timezone.utc),
        "submission_fiscal_year": 1600,
        "submission_fiscal_quarter": 1,
        "submission_fiscal_month": 3,
        "submission_reveal_date": datetime(1600, 1, 28, tzinfo=timezone.utc),
        "is_quarter": True,
    },
    {"model": ObjectClass, "id": 1},
    {"model": GTASSF133Balances, "fiscal_year": 1600, "fiscal_period": 3, "obligations_incurred_total_cpe": -10},
    {
        "model": SubmissionAttributes,
        "submission_id": -1,
        "reporting_fiscal_year": 1600,
        "reporting_fiscal_period": 3,
        "reporting_fiscal_quarter": 1,
    },
    {
        "model": ToptierAgency,
        "toptier_agency_id": -1,
        "name": "random_funding_name_1",
        "toptier_code": "random_funding_code_1",
    },
    {
        "model": ToptierAgency,
        "toptier_agency_id": -2,
        "name": "random_funding_name_2",
        "toptier_code": "random_funding_code_2",
    },
    {"model": Agency, "toptier_agency_id": -1, "toptier_flag": True},
    {"model": Agency, "toptier_agency_id": -2, "toptier_flag": True},
    {
        "model": TreasuryAppropriationAccount,
        "treasury_account_identifier": -1,
        "funding_toptier_agency_id": -1,
        "federal_account_id": 1,
    },
    {
        "model": FederalAccount,
        "id": 1,
        "account_title": "Tommy Two-Tone",
        "agency_identifier": "867",
        "main_account_code": "5309",
        "federal_account_code": "867-5309",
    },
    {
        "model": TreasuryAppropriationAccount,
        "treasury_account_identifier": -2,
        "funding_toptier_agency_id": -2,
        "federal_account_id": 1,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -1,
        "submission_id": -1,
        "treasury_account_id": -1,
        "obligations_incurred_by_program_object_class_cpe": -5,
        "deobligations_recoveries_refund_pri_program_object_class_cpe": 100,
        "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 1000,
        "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
        "prior_year_adjustment": "X",
        "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
        "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
        "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
        "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
        "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
        "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
        "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
        "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
        "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
        "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
        "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
        "ussgl480110_rein_undel_ord_cpe": 0,
        "ussgl490110_rein_deliv_ord_cpe": 0,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -2,
        "submission_id": -1,
        "treasury_account_id": -1,
        "obligations_incurred_by_program_object_class_cpe": -10,
        "deobligations_recoveries_refund_pri_program_object_class_cpe": 100,
        "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 1000,
        "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
        "prior_year_adjustment": "X",
        "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
        "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
        "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
        "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
        "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
        "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
        "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
        "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
        "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
        "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
        "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
        "ussgl480110_rein_undel_ord_cpe": 0,
        "ussgl490110_rein_deliv_ord_cpe": 0,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -3,
        "submission_id": -1,
        "treasury_account_id": -2,
        "obligations_incurred_by_program_object_class_cpe": -1,
        "object_class_id": 1,
        "deobligations_recoveries_refund_pri_program_object_class_cpe": 100,
        "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 1000,
        "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
        "prior_year_adjustment": "X",
        "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
        "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
        "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
        "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
        "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
        "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
        "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
        "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
        "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
        "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
        "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
        "ussgl480110_rein_undel_ord_cpe": 0,
        "ussgl490110_rein_deliv_ord_cpe": 0,
    },
]


@pytest.fixture
def setup_only_dabs_window():
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_quarter=1,
        is_quarter=True,
        submission_reveal_date="2017-05-01",
        period_start_date="2017-03-13",
        period_end_date="2017-04-13",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_quarter=3,
        is_quarter=True,
        submission_reveal_date="2017-12-01",
        period_start_date="2017-10-01",
        period_end_date="2017-09-01",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_month=3,
        is_quarter=False,
        submission_reveal_date="2017-05-01",
        period_start_date="2017-03-13",
        period_end_date="2017-04-13",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_reveal_date="2017-12-01",
        period_start_date="2017-10-01",
        period_end_date="2017-09-01",
    )


@pytest.mark.django_db
def test_unreported_data_actual_value_file_b(client):

    models = copy.deepcopy(GLOBAL_MOCK_DICT)
    for entry in models:
        baker.make(entry.pop("model"), **entry)

    json_request = {"type": "agency", "filters": {"fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    expected_results = {
        "total": -10,
        "agencies": ["Unreported Data", "random_funding_name_2", "random_funding_name_1"],
        "amounts": [6, -1, -15],
    }

    actual_results = {
        "total": json_response["total"],
        "agencies": [entry["name"] for entry in json_response["results"]],
        "amounts": [entry["amount"] for entry in json_response["results"]],
    }

    assert expected_results == actual_results


@pytest.mark.django_db
def test_unreported_data_actual_value_file_c(client):
    models_to_mock = [
        {
            "model": DABSSubmissionWindowSchedule,
            "id": 1600031,
            "period_end_date": datetime(1599, 12, 31, tzinfo=timezone.utc),
            "submission_fiscal_year": 1600,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": datetime(1600, 1, 28, tzinfo=timezone.utc),
            "is_quarter": True,
        },
        {"model": GTASSF133Balances, "fiscal_year": 1600, "fiscal_period": 3, "obligations_incurred_total_cpe": -10},
        {
            "model": SubmissionAttributes,
            "submission_id": -1,
            "reporting_fiscal_year": 1600,
            "reporting_fiscal_quarter": 1,
            "reporting_fiscal_period": 3,
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -1,
            "name": "random_funding_name_1",
            "toptier_code": "random_funding_code_1",
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -2,
            "name": "random_funding_name_2",
            "toptier_code": "random_funding_code_2",
        },
        {"model": Agency, "id": -1, "toptier_agency_id": -1, "toptier_flag": True},
        {"model": Agency, "id": -2, "toptier_agency_id": -2, "toptier_flag": True},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -1, "funding_toptier_agency_id": -1},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -2, "funding_toptier_agency_id": -2},
        {
            "model": TransactionSearch,
            "transaction_id": 1,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 2,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_2",
            "recipient_name_raw": "random_recipient_name_2",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 3,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 4,
            "is_fpds": True,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 5,
            "is_fpds": True,
            "recipient_name": "random_recipient_name_4",
            "recipient_name_raw": "random_recipient_name_4",
        },
        {
            "model": AwardSearch,
            "award_id": 1,
            "latest_transaction_id": 1,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -1,
            "submission_id": -1,
            "award_id": 1,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -2,
        },
        {
            "model": AwardSearch,
            "award_id": 2,
            "latest_transaction_id": 2,
            "recipient_name": "random_recipient_name_2",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -2,
            "submission_id": -1,
            "award_id": 2,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -3,
        },
        {
            "model": AwardSearch,
            "award_id": 3,
            "latest_transaction_id": 3,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -3,
            "submission_id": -1,
            "award_id": 3,
            "treasury_account_id": -2,
            "transaction_obligated_amount": -5,
        },
        {
            "model": AwardSearch,
            "award_id": 4,
            "latest_transaction_id": 4,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -4,
            "submission_id": -1,
            "award_id": 4,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -7,
        },
        {
            "model": AwardSearch,
            "award_id": 5,
            "latest_transaction_id": 5,
            "recipient_name": "random_recipient_name_4",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -5,
            "submission_id": -1,
            "award_id": 5,
            "treasury_account_id": -2,
            "transaction_obligated_amount": -11,
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    json_request = {"type": "recipient", "filters": {"agency": "-1", "fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    expected_results = {
        "total": -12,
        "agencies": ["random_recipient_name_2", "random_recipient_name_1"],
        "amounts": [-3, -9],
    }

    actual_results = {
        "total": json_response["total"],
        "agencies": [entry["name"] for entry in json_response["results"]],
        "amounts": [entry["amount"] for entry in json_response["results"]],
    }

    assert expected_results == actual_results


@pytest.mark.django_db
def test_unreported_data_no_data_available(client):
    json_request = {"type": "agency", "filters": {"fy": "1700", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "Fiscal parameters provided do not belong to a current submission period"


@pytest.mark.django_db
def test_federal_account_linkage(client):
    models = copy.deepcopy(GLOBAL_MOCK_DICT)
    for entry in models:
        baker.make(entry.pop("model"), **entry)
    json_request = {"type": "federal_account", "filters": {"fy": "1600", "quarter": "1"}}
    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))
    json_response = response.json()
    assert json_response["results"][0]["account_number"] == "867-5309"


@pytest.mark.django_db
def test_budget_function_filter_success(setup_only_dabs_window, client):

    # Test for Budget Function Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "budget_function", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Budget Sub Function Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "quarter": 1, "budget_function": "050"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {"fy": "2017", "quarter": 1, "budget_function": "050", "budget_subfunction": "053"},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                    "recipient": 13916,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_budget_function_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_object_class_filter_success(setup_only_dabs_window, client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_quarter=1,
        is_quarter=True,
        submission_reveal_date="2017-06-01",
        period_start_date="2017-04-01",
    )

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "object_class", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": 1, "object_class": "20"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "quarter": 1, "object_class": "20"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {"fy": "2017", "quarter": 1, "object_class": "20", "federal_account": 2358},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "object_class": "20",
                    "federal_account": 2358,
                    "program_activity": 15103,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "object_class": "20",
                    "federal_account": 2358,
                    "program_activity": 15103,
                    "recipient": 301773,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_object_class_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_agency_filter_success(setup_only_dabs_window, client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_quarter=1,
        is_quarter=True,
        submission_reveal_date="2017-06-01",
        period_start_date="2017-04-01",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_fiscal_quarter=3,
        is_quarter=True,
        submission_reveal_date="2017-09-01",
        period_start_date="2017-07-01",
    )

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": "3"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "program_activity", "filters": {"fy": "2017", "quarter": 1, "federal_account": 1500}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {"fy": "2017", "quarter": 1, "federal_account": 1500, "program_activity": 12697},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_agency_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "23", "quarter": "3"}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_object_budget_match(client):

    models = copy.deepcopy(GLOBAL_MOCK_DICT)
    for entry in models:
        baker.make(entry.pop("model"), **entry)
    baker.make(
        FinancialAccountsByProgramActivityObjectClass,
        **{
            "financial_accounts_by_program_activity_object_class_id": -4,
            "submission_id": -1,
            "treasury_account_id": -1,
            "obligations_incurred_by_program_object_class_cpe": -5,
            "object_class_id": 1,
        },
    )

    json_request = {"type": "budget_function", "filters": {"fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response_1 = response.json()

    json_request = {"type": "object_class", "filters": {"fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))
    json_response_2 = response.json()
    assert json_response_1["results"][0]["amount"] == json_response_2["results"][0]["amount"]


@pytest.mark.django_db
def test_period(setup_only_dabs_window, client):

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "period": 3}}),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": "3"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "period": "9"}}),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Agency Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "quarter": 1}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "period": 3}}),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "program_activity", "filters": {"fy": "2017", "quarter": 1, "federal_account": 1500}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "program_activity", "filters": {"fy": "2017", "period": 3, "federal_account": 1500}}),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {"fy": "2017", "quarter": 1, "federal_account": 1500, "program_activity": 12697},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {"fy": "2017", "period": 3, "federal_account": 1500, "program_activity": 12697},
            }
        ),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "period": 3,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                },
            }
        ),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "quarter": 1,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp2 = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "period": 3,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917,
                },
            }
        ),
    )
    assert resp2.status_code == status.HTTP_200_OK
    assert resp.json() == resp2.json()


@pytest.mark.django_db
def test_unreported_file_c(client):
    models_to_mock = [
        {
            "model": DABSSubmissionWindowSchedule,
            "id": 1600031,
            "period_end_date": datetime(1599, 12, 31, tzinfo=timezone.utc),
            "submission_fiscal_year": 1600,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": datetime(1600, 1, 28, tzinfo=timezone.utc),
            "is_quarter": True,
        },
        {"model": GTASSF133Balances, "fiscal_year": 1600, "fiscal_period": 3, "obligations_incurred_total_cpe": -10},
        {
            "model": SubmissionAttributes,
            "submission_id": -1,
            "reporting_fiscal_year": 1600,
            "reporting_fiscal_quarter": 1,
            "reporting_fiscal_period": 3,
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -1,
            "name": "random_funding_name_1",
            "toptier_code": "random_funding_code_1",
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -2,
            "name": "random_funding_name_2",
            "toptier_code": "random_funding_code_2",
        },
        {"model": Agency, "id": -1, "toptier_agency_id": -1, "toptier_flag": True},
        {"model": Agency, "id": -2, "toptier_agency_id": -2, "toptier_flag": True},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -1, "funding_toptier_agency_id": -1},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -2, "funding_toptier_agency_id": -2},
        {
            "model": FinancialAccountsByProgramActivityObjectClass,
            "financial_accounts_by_program_activity_object_class_id": -1,
            "submission_id": -1,
            "treasury_account_id": -1,
            "obligations_incurred_by_program_object_class_cpe": -5,
            "deobligations_recoveries_refund_pri_program_object_class_cpe": 0,
            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 0,
            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
            "prior_year_adjustment": "X",
            "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
            "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
            "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
            "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
            "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
            "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
            "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
            "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
            "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
            "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
            "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
            "ussgl480110_rein_undel_ord_cpe": 0,
            "ussgl490110_rein_deliv_ord_cpe": 0,
        },
        {
            "model": FinancialAccountsByProgramActivityObjectClass,
            "financial_accounts_by_program_activity_object_class_id": -2,
            "submission_id": -1,
            "treasury_account_id": -1,
            "obligations_incurred_by_program_object_class_cpe": -5,
            "deobligations_recoveries_refund_pri_program_object_class_cpe": 0,
            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 0,
            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
            "prior_year_adjustment": "X",
            "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
            "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
            "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
            "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
            "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
            "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
            "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
            "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
            "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
            "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
            "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
            "ussgl480110_rein_undel_ord_cpe": 0,
            "ussgl490110_rein_deliv_ord_cpe": 0,
        },
        {
            "model": FinancialAccountsByProgramActivityObjectClass,
            "financial_accounts_by_program_activity_object_class_id": -3,
            "submission_id": -1,
            "treasury_account_id": -1,
            "obligations_incurred_by_program_object_class_cpe": -5,
            "deobligations_recoveries_refund_pri_program_object_class_cpe": 0,
            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": 0,
            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": 0,
            "prior_year_adjustment": "X",
            "ussgl480100_undelivered_orders_obligations_unpaid_cpe": 0,
            "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": 0,
            "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": 0,
            "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": 0,
            "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": 0,
            "ussgl490100_delivered_orders_obligations_unpaid_cpe": 0,
            "ussgl490200_delivered_orders_obligations_paid_cpe": 0,
            "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": 0,
            "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": 0,
            "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": 0,
            "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": 0,
            "ussgl480110_rein_undel_ord_cpe": 0,
            "ussgl490110_rein_deliv_ord_cpe": 0,
        },
        {
            "model": TransactionSearch,
            "transaction_id": 1,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 2,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_2",
            "recipient_name_raw": "random_recipient_name_2",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 3,
            "is_fpds": False,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 4,
            "is_fpds": True,
            "recipient_name": "random_recipient_name_1",
            "recipient_name_raw": "random_recipient_name_1",
        },
        {
            "model": TransactionSearch,
            "transaction_id": 5,
            "is_fpds": True,
            "recipient_name": "random_recipient_name_4",
            "recipient_name_raw": "random_recipient_name_4",
        },
        {
            "model": AwardSearch,
            "award_id": 1,
            "latest_transaction_id": 1,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -1,
            "submission_id": -1,
            "award_id": 1,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -2,
        },
        {
            "model": AwardSearch,
            "award_id": 2,
            "latest_transaction_id": 2,
            "recipient_name": "random_recipient_name_2",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -2,
            "submission_id": -1,
            "award_id": 2,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -3,
        },
        {
            "model": AwardSearch,
            "award_id": 3,
            "latest_transaction_id": 3,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -3,
            "submission_id": -1,
            "award_id": 3,
            "treasury_account_id": -2,
            "transaction_obligated_amount": -5,
        },
        {
            "model": AwardSearch,
            "award_id": 4,
            "latest_transaction_id": 4,
            "recipient_name": "random_recipient_name_1",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -4,
            "submission_id": -1,
            "award_id": 4,
            "treasury_account_id": -1,
            "transaction_obligated_amount": -7,
        },
        {
            "model": AwardSearch,
            "award_id": 5,
            "latest_transaction_id": 5,
            "recipient_name": "random_recipient_name_4",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -5,
            "submission_id": -1,
            "award_id": 5,
            "treasury_account_id": -2,
            "transaction_obligated_amount": -11,
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    json_request = {"type": "recipient", "filters": {"agency": "-1", "fy": "1600", "quarter": "1"}}
    resp = client.post("/api/v2/spending/", content_type="application/json", data=json_request)
    json_request2 = {"type": "object_class", "filters": {"agency": "-1", "fy": "1600", "quarter": "1"}}
    resp2 = client.post("/api/v2/spending/", content_type="application/json", data=json_request2)
    assert resp.status_code == status.HTTP_200_OK
    assert resp2.status_code == status.HTTP_200_OK
    response = resp.json()
    response2 = resp2.json()
    expected_results = {
        "total": -12,
        "agencies": ["random_recipient_name_2", "random_recipient_name_1"],
        "amounts": [-3, -9],
    }

    actual_results = {
        "total": response["total"],
        "agencies": [entry["name"] for entry in response["results"]],
        "amounts": [entry["amount"] for entry in response["results"]],
    }
    assert expected_results == actual_results
    # "Non-Award Spending" was removed as requested by DEV-7066
    #   This assertion is to ensure it's not unintentionally added back
    assert "Non-Award Spending" not in actual_results
    # After removing "Non-Award Spending" from recipient aggregation
    #   we don't expect object_class and recipient to total to the same number.
    # The sum of amounts on the breakdown by recipient will still equal
    #   the sum of the awards for that recipient
    assert response["total"] != response2["total"]
    assert response["total"] == -12
    assert response2["total"] == -15
