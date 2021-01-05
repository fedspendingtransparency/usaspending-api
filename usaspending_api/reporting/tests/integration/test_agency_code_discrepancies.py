import pytest
from model_mommy import mommy
from rest_framework import status


url = "/api/v2/reporting/agencies/123/discrepancies/?fiscal_year=2020&fiscal_period=2"


@pytest.fixture
def setup_test_data(db):
    """ Insert test data into DB """
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 1",
        obligated_amount=10.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 2",
        obligated_amount=1.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=321,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 2",
        obligated_amount=12.0,
    )


def test_basic_success(setup_test_data, client):
    resp = client.get("/api/v2/reporting/agencies/123/discrepancies/?fiscal_year=2020&fiscal_period=3")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0

    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {"tas": "TAS 1", "amount": 10.0,},
        {"tas": "TAS 2", "amount": 1.0,},
    ]
    assert response["results"] == expected_results


# def test_pagination(setup_test_data, client):
#     resp = client.get(url + "?sort=current_total_budget_authority_amount&order=asc")
#     assert resp.status_code == status.HTTP_200_OK
#     response = resp.json()
#     assert len(response["results"]) == 3
#     expected_results = [
#         {
#             "fiscal_year": 2020,
#             "fiscal_period": 12,
#             "current_total_budget_authority_amount": 100.0,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 18.6,
#                 "tas_accounts_total": 100.00,
#                 "tas_obligation_not_in_gtas_total": 12.0,
#                 "missing_tas_accounts_count": 1,
#             },
#             "obligation_difference": 0.0,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 6,
#             "current_total_budget_authority_amount": 22478810.97,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.03,
#                 "tas_accounts_total": 200.00,
#                 "tas_obligation_not_in_gtas_total": 11.0,
#                 "missing_tas_accounts_count": 2,
#             },
#             "obligation_difference": 84931.95,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 9,
#             "current_total_budget_authority_amount": 22478810.98,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.04,
#                 "tas_accounts_total": None,
#                 "tas_obligation_not_in_gtas_total": 0.0,
#                 "missing_tas_accounts_count": 0,
#             },
#             "obligation_difference": 84931.96,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#     ]
#     assert response["results"] == expected_results

#     resp = client.get(url + "?limit=1")
#     response = resp.json()
#     assert len(response["results"]) == 1
#     expected_results = [
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 9,
#             "current_total_budget_authority_amount": 22478810.98,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.04,
#                 "tas_accounts_total": None,
#                 "tas_obligation_not_in_gtas_total": 0.0,
#                 "missing_tas_accounts_count": 0,
#             },
#             "obligation_difference": 84931.96,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         }
#     ]
#     assert response["results"] == expected_results

#     resp = client.get(url + "?limit=1&page=2")
#     response = resp.json()
#     assert len(response["results"]) == 1
#     expected_results = [
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 6,
#             "current_total_budget_authority_amount": 22478810.97,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.03,
#                 "tas_accounts_total": 200.00,
#                 "tas_obligation_not_in_gtas_total": 11.0,
#                 "missing_tas_accounts_count": 2,
#             },
#             "obligation_difference": 84931.95,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         }
#     ]
#     assert response["results"] == expected_results


# def test_secondary_sort(setup_test_data, client):
#     resp = client.get(url + "?sort=fiscal_year&order=asc")
#     assert resp.status_code == status.HTTP_200_OK
#     response = resp.json()
#     assert len(response["results"]) == 3
#     expected_results = [
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 6,
#             "current_total_budget_authority_amount": 22478810.97,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.03,
#                 "tas_accounts_total": 200.00,
#                 "tas_obligation_not_in_gtas_total": 11.0,
#                 "missing_tas_accounts_count": 2,
#             },
#             "obligation_difference": 84931.95,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#         {
#             "fiscal_year": 2019,
#             "fiscal_period": 9,
#             "current_total_budget_authority_amount": 22478810.98,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 1788370.04,
#                 "tas_accounts_total": None,
#                 "tas_obligation_not_in_gtas_total": 0.0,
#                 "missing_tas_accounts_count": 0,
#             },
#             "obligation_difference": 84931.96,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#         {
#             "fiscal_year": 2020,
#             "fiscal_period": 12,
#             "current_total_budget_authority_amount": 100.0,
#             "recent_publication_date": None,
#             "recent_publication_date_certified": False,
#             "tas_account_discrepancies_totals": {
#                 "gtas_obligation_total": 18.6,
#                 "tas_accounts_total": 100.00,
#                 "tas_obligation_not_in_gtas_total": 12.0,
#                 "missing_tas_accounts_count": 1,
#             },
#             "obligation_difference": 0.0,
#             "unlinked_contract_award_count": 0,
#             "unlinked_assistance_award_count": 0,
#         },
#     ]
#     assert response["results"] == expected_results
