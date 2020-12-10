import pytest
from model_mommy import mommy
from rest_framework import status


url = "/api/v2/reporting/agencies/123/overview/"


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = mommy.make(
        "submissions.SubmissionAttributes", submission_id=1, reporting_fiscal_year=2019, reporting_fiscal_period=6
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes", submission_id=2, reporting_fiscal_year=2020, reporting_fiscal_period=12,
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


def test_basic_success(setup_test_data, client):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
        },
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
        },
    ]
    assert response["results"] == expected_results


def test_pagination(setup_test_data, client):
    resp = client.get(url + "?sort=current_total_budget_authority_amount&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
        },
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?limit=1")
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "fiscal_year": 2019,
            "fiscal_period": 6,
            "current_total_budget_authority_amount": 22478810.97,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
        }
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?limit=1&page=2")
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [
        {
            "fiscal_year": 2020,
            "fiscal_period": 12,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 18.6,
                "tas_accounts_total": 100.00,
                "missing_tas_accounts_count": 1,
            },
            "obligation_difference": 0.0,
        }
    ]
    assert response["results"] == expected_results
