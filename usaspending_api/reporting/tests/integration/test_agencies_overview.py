import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.helpers.fiscal_year_helpers import (
    current_fiscal_year,
    calculate_last_completed_fiscal_quarter,
    get_final_period_of_quarter,
)

url = "/api/v2/reporting/agencies/overview/"


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = mommy.make(
        "submissions.SubmissionAttributes", submission_id=1, reporting_fiscal_year=2019, reporting_fiscal_period=6
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        reporting_fiscal_year=current_fiscal_year(),
        reporting_fiscal_period=get_final_period_of_quarter(
            calculate_last_completed_fiscal_quarter(current_fiscal_year())
        ),
    )
    mommy.make("references.Agency", id=1, toptier_agency_id=1, toptier_flag=True)
    mommy.make("references.Agency", id=2, toptier_agency_id=2, toptier_flag=True)
    mommy.make("references.Agency", id=3, toptier_agency_id=3, toptier_flag=True)
    agencies = [
        mommy.make(
            "references.ToptierAgency", toptier_agency_id=1, toptier_code="123", abbreviation="ABC", name="Test Agency"
        ),
        mommy.make(
            "references.ToptierAgency",
            toptier_agency_id=2,
            toptier_code="987",
            abbreviation="XYZ",
            name="Test Agency 2",
        ),
        mommy.make(
            "references.ToptierAgency",
            toptier_agency_id=3,
            toptier_code="001",
            abbreviation="AAA",
            name="Test Agency 3",
        ),
    ]

    treas_accounts = [
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency_id=agencies[0].toptier_agency_id,
            tas_rendering_label="tas-1-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency_id=agencies[2].toptier_agency_id,
            tas_rendering_label="tas-2-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            funding_toptier_agency_id=agencies[1].toptier_agency_id,
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
            "toptier_code": agencies[0].toptier_code,
            "diff": 29.5,
        },
        {
            "year": sub2.reporting_fiscal_year,
            "period": sub2.reporting_fiscal_period,
            "label": treas_accounts[1].tas_rendering_label,
            "toptier_code": agencies[2].toptier_code,
            "diff": -1.3,
        },
        {
            "year": sub2.reporting_fiscal_year,
            "period": sub2.reporting_fiscal_period,
            "label": treas_accounts[2].tas_rendering_label,
            "toptier_code": agencies[1].toptier_code,
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
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=2,
        toptier_code=987,
        fiscal_year=current_fiscal_year(),
        fiscal_period=get_final_period_of_quarter(calculate_last_completed_fiscal_quarter(current_fiscal_year())),
        total_dollars_obligated_gtas=18.6,
        total_budgetary_resources=100,
        total_diff_approp_ocpa_obligated_amounts=0,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        reporting_agency_overview_id=3,
        toptier_code="001",
        fiscal_year=current_fiscal_year(),
        fiscal_period=get_final_period_of_quarter(calculate_last_completed_fiscal_quarter(current_fiscal_year())),
        total_dollars_obligated_gtas=20.0,
        total_budgetary_resources=10.0,
        total_diff_approp_ocpa_obligated_amounts=10.0,
    )


def test_basic_success(setup_test_data, client):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "agency_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 0.0,
        },
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "agency_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 10.0,
        },
    ]
    assert response["results"] == expected_results


def test_filter(setup_test_data, client):
    resp = client.get(url + "?filter=Test Agency 2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "agency_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 0.0,
        }
    ]
    assert response["results"] == expected_results


def test_pagination(setup_test_data, client):
    resp = client.get(url + "?limit=1")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "agency_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 0.0,
        }
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?limit=1&page=2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "agency_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 10.0,
        }
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?sort=obligation_difference&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "agency_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 10.0,
        },
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "agency_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": None,
            },
            "obligation_difference": 0.0,
        },
    ]
    assert response["results"] == expected_results


def test_fiscal_year_period_selection(setup_test_data, client):
    resp = client.get(url + "?fiscal_year=2019&fiscal_period=6")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
