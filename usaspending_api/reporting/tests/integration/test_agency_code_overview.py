import pytest

from django.conf import settings
from model_mommy import mommy
from rest_framework import status

from usaspending_api.agency.v2.views.agency_base import AgencyBase

url = "/api/v2/reporting/agencies/123/overview/"


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="123",
        quarter_format_flag=False,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=6,
        published_date="2019-07-03",
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="123",
        quarter_format_flag=False,
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        published_date="2021-02-11",
    )
    agency = mommy.make("references.ToptierAgency", toptier_code="123", abbreviation="ABC", name="Test Agency")

    treas_accounts = [
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency_id=agency.toptier_agency_id,
            tas_rendering_label="tas-1-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency_id=agency.toptier_agency_id,
            tas_rendering_label="tas-2-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            funding_toptier_agency_id=agency.toptier_agency_id,
            tas_rendering_label="tas-3-overview",
        ),
    ]
    approps = [
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[0], "total_resources": 50},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[1], "total_resources": 12},
        {"sub_id": sub2.submission_id, "treasury_account": treas_accounts[1], "total_resources": 29},
        {"sub_id": sub2.submission_id, "treasury_account": treas_accounts[2], "total_resources": 15.5},
    ]
    for approp in approps:
        mommy.make(
            "accounts.AppropriationAccountBalances",
            submission_id=approp["sub_id"],
            treasury_account_identifier=approp["treasury_account"],
            total_budgetary_resources_amount_cpe=approp["total_resources"],
        )

    reporting_tases = [
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "label": treas_accounts[0].tas_rendering_label,
            "toptier_code": agency.toptier_code,
            "diff": 29.5,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "label": treas_accounts[1].tas_rendering_label,
            "toptier_code": agency.toptier_code,
            "diff": -1.3,
        },
        {
            "year": sub2.reporting_fiscal_year,
            "period": sub2.reporting_fiscal_period,
            "label": treas_accounts[2].tas_rendering_label,
            "toptier_code": agency.toptier_code,
            "diff": 20.5,
        },
    ]
    for reporting_tas in reporting_tases:
        mommy.make(
            "reporting.ReportingAgencyTas",
            fiscal_year=reporting_tas["year"],
            fiscal_period=reporting_tas["period"],
            tas_rendering_label=reporting_tas["label"],
            toptier_code=reporting_tas["toptier_code"],
            diff_approp_ocpa_obligated_amounts=reporting_tas["diff"],
            appropriation_obligated_amount=100,
        )

    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=1,
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=6,
        total_dollars_obligated_gtas=1788370.03,
        total_budgetary_resources=22478810.97,
        total_diff_approp_ocpa_obligated_amounts=84931.95,
        unlinked_procurement_c_awards=1,
        unlinked_assistance_c_awards=2,
        unlinked_procurement_d_awards=3,
        unlinked_assistance_d_awards=4,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=2,
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=12,
        total_dollars_obligated_gtas=18.6,
        total_budgetary_resources=100,
        total_diff_approp_ocpa_obligated_amounts=0,
        unlinked_procurement_c_awards=10,
        unlinked_assistance_c_awards=20,
        unlinked_procurement_d_awards=30,
        unlinked_assistance_d_awards=40,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=3,
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=9,
        total_dollars_obligated_gtas=1788370.04,
        total_budgetary_resources=22478810.98,
        total_diff_approp_ocpa_obligated_amounts=84931.96,
        unlinked_procurement_c_awards=100,
        unlinked_assistance_c_awards=200,
        unlinked_procurement_d_awards=300,
        unlinked_assistance_d_awards=400,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=6,
        tas_rendering_label="TAS 1",
        obligated_amount=10.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=6,
        tas_rendering_label="TAS 2",
        obligated_amount=1.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=12,
        tas_rendering_label="TAS 2",
        obligated_amount=12.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=12,
        tas_rendering_label="TAS 3",
        obligated_amount=0,
    )
    mommy.make(
        "references.GTASSF133Balances",
        id=1,
        fiscal_year=2019,
        fiscal_period=6,
        total_budgetary_resources_cpe=200000000,
    )
    mommy.make(
        "references.GTASSF133Balances",
        id=2,
        fiscal_year=2019,
        fiscal_period=9,
        total_budgetary_resources_cpe=150000000,
    )
    mommy.make(
        "references.GTASSF133Balances",
        id=3,
        fiscal_year=2020,
        fiscal_period=12,
        total_budgetary_resources_cpe=100000000,
    )
    mommy.make(
        "references.GTASSF133Balances",
        id=4,
        fiscal_year=2019,
        fiscal_period=10,
        total_budgetary_resources_cpe=10,
    )


assurance_statement_2019_6 = f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/2019/P06/123%20-%20Test%20Agency%20(ABC)/2019-P06-123_Test%20Agency%20(ABC)-Assurance_Statement.txt"
assurance_statement_2020_12 = f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/2020/P12/123%20-%20Test%20Agency%20(ABC)/2020-P12-123_Test%20Agency%20(ABC)-Assurance_Statement.txt"
assurance_statement_quarter = f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/2019/Q3/123%20-%20Quarterly%20Agency%20(QA)/2019-Q3-123_Quarterly%20Agency%20(QA)-Assurance_Statement.txt"


def test_basic_success(setup_test_data, client):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000,
            "percent_of_total_budgetary_resources": 0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
    ]
    assert response["results"] == expected_results


def test_pagination(setup_test_data, client):
    resp = client.get(url + "?sort=current_total_budget_authority_amount&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000,
            "percent_of_total_budgetary_resources": 0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?limit=1")
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        }
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?limit=1&page=2")
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        }
    ]
    assert response["results"] == expected_results


def test_secondary_sort(setup_test_data, client):
    resp = client.get(url + "?sort=fiscal_year&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000,
            "percent_of_total_budgetary_resources": 0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?sort=percent_of_total_budgetary_resources&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000,
            "percent_of_total_budgetary_resources": 0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
    ]
    assert response["results"] == expected_results


def test_quarterly_assurance_statements():
    results = {
        "agency_name": "Quarterly Agency",
        "abbreviation": "QA",
        "toptier_code": "123",
        "fiscal_year": 2019,
        "fiscal_period": 9,
        "submission_is_quarter": True,
    }

    assurance_statement = AgencyBase.create_assurance_statement_url(results)

    assert assurance_statement == assurance_statement_quarter


def test_secondary_period_sort(setup_test_data, client):
    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=4,
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=10,
        total_dollars_obligated_gtas=0.0,
        total_budgetary_resources=0.0,
        total_diff_approp_ocpa_obligated_amounts=0.0,
        unlinked_procurement_c_awards=1,
        unlinked_assistance_c_awards=2,
        unlinked_procurement_d_awards=3,
        unlinked_assistance_d_awards=4,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2019,
        fiscal_period=10,
        tas_rendering_label="TAS 2",
        obligated_amount=1000,
    )
    resp = client.get(url + "?sort=fiscal_year&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 4
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000.0,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000.0,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 10,
            "current_total_budget_authority_amount": 0.0,
            "total_budgetary_resources": 10.0,
            "percent_of_total_budgetary_resources": 0.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 0.0,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 1000,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000.0,
            "percent_of_total_budgetary_resources": 0.0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?sort=fiscal_year&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 4
    expected_results = [
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "total_budgetary_resources": 100000000.0,
            "percent_of_total_budgetary_resources": 0.0,
            "recent_publication_date": "2021-02-11T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 12.0,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 40,
            "unlinked_assistance_award_count": 60,
            "assurance_statement_url": assurance_statement_2020_12,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 10,
            "current_total_budget_authority_amount": 0.0,
            "total_budgetary_resources": 10.0,
            "percent_of_total_budgetary_resources": 0.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 0.0,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 1000,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 9,
            "current_total_budget_authority_amount": 22478810.98,
            "total_budgetary_resources": 150000000.0,
            "percent_of_total_budgetary_resources": 14.99,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.04,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 84931.96,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": None,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "total_budgetary_resources": 200000000.0,
            "percent_of_total_budgetary_resources": 11.24,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 200.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_2019_6,
        },
    ]
    assert response["results"] == expected_results
