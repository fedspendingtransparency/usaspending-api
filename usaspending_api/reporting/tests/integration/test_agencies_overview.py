import pytest

from django.conf import settings
from model_mommy import mommy
from rest_framework import status
from datetime import datetime, timezone

url = "/api/v2/reporting/agencies/overview/"

CURRENT_FISCAL_YEAR = 2020
CURRENT_LAST_QUARTER = 1
CURRENT_LAST_PERIOD = 3

assurance_statement_1 = (
    f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/"
    "2019/P06/123%20-%20Test%20Agency%20(ABC)/2019-P06-123_Test%20Agency%20(ABC)-Assurance_Statement.txt"
)
assurance_statement_2 = (
    f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/"
    f"{CURRENT_FISCAL_YEAR}/P{CURRENT_LAST_PERIOD:02}/987%20-%20Test%20Agency%202%20(XYZ)/"
    f"{CURRENT_FISCAL_YEAR}-P{CURRENT_LAST_PERIOD:02}-987_Test%20Agency%202%20(XYZ)-Assurance_Statement.txt"
)
assurance_statement_3 = (
    f"{settings.FILES_SERVER_BASE_URL}/agency_submissions/Raw%20DATA%20Act%20Files/"
    f"{CURRENT_FISCAL_YEAR}/Q{CURRENT_LAST_QUARTER}/001%20-%20Test%20Agency%203%20(AAA)/"
    f"{CURRENT_FISCAL_YEAR}-Q{CURRENT_LAST_QUARTER}-001_Test%20Agency%203%20(AAA)-Assurance_Statement.txt"
)


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
        toptier_code="987",
        quarter_format_flag=False,
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=CURRENT_LAST_PERIOD,
        published_date=f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07",
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        toptier_code="001",
        quarter_format_flag=True,
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=CURRENT_LAST_PERIOD,
        published_date=f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07",
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
            awarding_toptier_agency_id=agencies[0].toptier_agency_id,
            tas_rendering_label="tas-1-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            awarding_toptier_agency_id=agencies[2].toptier_agency_id,
            tas_rendering_label="tas-2-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            awarding_toptier_agency_id=agencies[1].toptier_agency_id,
            tas_rendering_label="tas-3-overview",
        ),
    ]
    approps = [
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[0], "total_resources": 50},
        {"sub_id": sub3.submission_id, "treasury_account": treas_accounts[1], "total_resources": 12},
        {"sub_id": sub3.submission_id, "treasury_account": treas_accounts[1], "total_resources": 29},
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
        toptier_code="123",
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
        toptier_code="987",
        fiscal_year=CURRENT_FISCAL_YEAR,
        fiscal_period=CURRENT_LAST_PERIOD,
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
        toptier_code="001",
        fiscal_year=CURRENT_FISCAL_YEAR,
        fiscal_period=CURRENT_LAST_PERIOD,
        total_dollars_obligated_gtas=20.0,
        total_budgetary_resources=10.0,
        total_diff_approp_ocpa_obligated_amounts=10.0,
        unlinked_procurement_c_awards=100,
        unlinked_assistance_c_awards=200,
        unlinked_procurement_d_awards=300,
        unlinked_assistance_d_awards=400,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code="123",
        fiscal_year=2019,
        fiscal_period=6,
        tas_rendering_label="TAS 1",
        obligated_amount=10.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code="123",
        fiscal_year=2019,
        fiscal_period=6,
        tas_rendering_label="TAS 2",
        obligated_amount=1.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code="987",
        fiscal_year=CURRENT_FISCAL_YEAR,
        fiscal_period=CURRENT_LAST_PERIOD,
        tas_rendering_label="TAS 2",
        obligated_amount=12.0,
    )
    mommy.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code="987",
        fiscal_year=CURRENT_FISCAL_YEAR,
        fiscal_period=CURRENT_LAST_PERIOD,
        tas_rendering_label="TAS 3",
        obligated_amount=0,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_reveal_date=datetime.now(timezone.utc),
        submission_fiscal_quarter=CURRENT_LAST_QUARTER,
    )


def test_basic_success(setup_test_data, client):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
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
            "assurance_statement_url": assurance_statement_2,
        },
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "toptier_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 10.0,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": assurance_statement_3,
        },
        {
            "agency_name": "Test Agency",
            "abbreviation": "ABC",
            "toptier_code": "123",
            "agency_id": 1,
            "current_total_budget_authority_amount": None,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        },
    ]
    assert response["results"] == expected_results


def test_filter(setup_test_data, client):
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
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
            "assurance_statement_url": assurance_statement_2,
        }
    ]

    resp = client.get(url + "?filter=Test Agency 2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == expected_results

    resp = client.get(url + "?filter=xYz")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
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
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
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
            "assurance_statement_url": assurance_statement_2,
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
            "toptier_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": "2021-07-07T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 10.0,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": assurance_statement_3,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?sort=obligation_difference&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "toptier_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 10.0,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": assurance_statement_3,
        },
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
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
            "assurance_statement_url": assurance_statement_2,
        },
        {
            "agency_name": "Test Agency",
            "abbreviation": "ABC",
            "toptier_code": "123",
            "agency_id": 1,
            "current_total_budget_authority_amount": None,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        },
    ]
    assert response["results"] == expected_results

    resp = client.get(url + "?sort=unlinked_assistance_award_count&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": 100.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
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
            "assurance_statement_url": assurance_statement_2,
        },
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "toptier_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": 10.0,
            "recent_publication_date": f"{CURRENT_FISCAL_YEAR}-{CURRENT_LAST_PERIOD+1:02}-07T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 20.0,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": 10.0,
            "unlinked_contract_award_count": 400,
            "unlinked_assistance_award_count": 600,
            "assurance_statement_url": assurance_statement_3,
        },
        {
            "agency_name": "Test Agency",
            "abbreviation": "ABC",
            "toptier_code": "123",
            "agency_id": 1,
            "current_total_budget_authority_amount": None,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        },
    ]
    assert response["results"] == expected_results


def test_fiscal_year_period_selection(setup_test_data, client):
    resp = client.get(url + "?fiscal_year=2019&fiscal_period=6")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 3

    expected_results = [
        {
            "agency_name": "Test Agency",
            "abbreviation": "ABC",
            "toptier_code": "123",
            "agency_id": 1,
            "current_total_budget_authority_amount": 22478810.97,
            "recent_publication_date": "2019-07-03T00:00:00Z",
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": 1788370.03,
                "tas_accounts_total": 100.00,
                "tas_obligation_not_in_gtas_total": 11.0,
                "missing_tas_accounts_count": 2,
            },
            "obligation_difference": 84931.95,
            "unlinked_contract_award_count": 4,
            "unlinked_assistance_award_count": 6,
            "assurance_statement_url": assurance_statement_1,
        },
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "XYZ",
            "toptier_code": "987",
            "agency_id": 2,
            "current_total_budget_authority_amount": None,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        },
        {
            "agency_name": "Test Agency 3",
            "abbreviation": "AAA",
            "toptier_code": "001",
            "agency_id": 3,
            "current_total_budget_authority_amount": None,
            "recent_publication_date": None,
            "recent_publication_date_certified": False,
            "tas_account_discrepancies_totals": {
                "gtas_obligation_total": None,
                "tas_accounts_total": None,
                "tas_obligation_not_in_gtas_total": 0.0,
                "missing_tas_accounts_count": 0,
            },
            "obligation_difference": None,
            "unlinked_contract_award_count": None,
            "unlinked_assistance_award_count": None,
            "assurance_statement_url": None,
        },
    ]
    assert response["results"] == expected_results
