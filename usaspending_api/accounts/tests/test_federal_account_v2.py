import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def fixture_data(db):
    mommy.make("references.ToptierAgency", cgac_code="001", abbreviation="ABCD", name="Dept. of Depts")
    mommy.make("references.ToptierAgency", cgac_code="002", abbreviation="EFGH", name="The Bureau")
    mommy.make("references.ToptierAgency", cgac_code="1601", abbreviation="DOL", name="Department of Labor")
    mommy.make("references.ToptierAgency", cgac_code="097", abbreviation="DOD", name="Department of Defense")
    mommy.make("references.ToptierAgency", cgac_code="021", abbreviation="DOD", name="Department of Navy")
    fa0 = mommy.make(FederalAccount, agency_identifier="001", main_account_code="0005", account_title="Something")
    fa1 = mommy.make(FederalAccount, agency_identifier="002", main_account_code="0005", account_title="Nothing1")
    fa2 = mommy.make(FederalAccount, agency_identifier="1600", main_account_code="0005", account_title="Nothing2")
    fa3 = mommy.make(FederalAccount, agency_identifier="097", main_account_code="0005", account_title="CGAC_DOD")
    fa4 = mommy.make(FederalAccount, agency_identifier="021", main_account_code="0005", account_title="CGAC_DOD(NAVY)")
    ta0 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa0)
    ta1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)
    ta2 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa2)
    ta3 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa3)
    ta4 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa4)

    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=1000,
        submission__reporting_period_start="2017-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=False,  # so filter it out
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=100,
        submission__reporting_period_start="2017-03-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2017-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta1,
        total_budgetary_resources_amount_cpe=9000,
        submission__reporting_period_start="2017-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta1,
        total_budgetary_resources_amount_cpe=500,
        submission__reporting_period_start="2016-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier__treasury_account_identifier="999",
        total_budgetary_resources_amount_cpe=4000,
        submission__reporting_period_start="2017-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta2,
        total_budgetary_resources_amount_cpe=1000,
        submission__reporting_period_start="2015-06-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta3,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2018-03-01",
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta4,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2018-03-02",
    )


@pytest.mark.django_db
def test_federal_accounts_endpoint_exists(client, fixture_data):
    """ Verify the federal accounts endpoint returns a status of 200 """
    resp = client.post(
        "/api/v2/federal_accounts/", content_type="application/json", data=json.dumps({"filters": {"fy": "2017"}})
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_federal_accounts_endpoint_correct_form(client, fixture_data):
    """ Verify the correct keys exist within the response """
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
    """ Verify federal accounts endpoint returns the correct data """
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"sort": {"field": "managing_agency", "direction": "asc"}, "filters": {"fy": "2017"}}),
    )
    response_data = resp.json()
    assert response_data["results"][0]["budgetary_resources"] == 3000
    assert response_data["results"][0]["managing_agency"] == "Dept. of Depts"
    assert response_data["results"][0]["managing_agency_acronym"] == "ABCD"

    assert response_data["results"][1]["managing_agency_acronym"] == "EFGH"
    assert response_data["results"][1]["budgetary_resources"] == 9000
    assert response_data["fy"] == "2017"


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
    response_data["results"][0]["managing_agency_acronym"] == "DOL"


@pytest.mark.django_db
def test_federal_account_content(client, fixture_data):
    """ Verify the correct Federal Account is returned with the correct contents"""
    resp = client.get("/api/v2/federal_accounts/001-0005/")

    response_data = resp.json()
    assert response_data["agency_identifier"] == "001"
    assert response_data["main_account_code"] == "0005"
    assert response_data["account_title"] == "Something"
    assert response_data["federal_account_code"] == "001-0005"


@pytest.mark.django_db
def test_federal_account_invalid_param(client, fixture_data):
    """ Verify the an invalid federal account code will return as a 400 """
    resp = client.get("/api/v2/federal_accounts/001-0006/")

    assert resp.status_code == 400


@pytest.mark.django_db
def test_federal_account_dod_cgac(client, fixture_data):
    """ Verify DOD CGAC query returns CGAC code for all DOD departments in addition to DOD's '097' """
    resp = client.post(
        "/api/v2/federal_accounts/",
        content_type="application/json",
        data=json.dumps({"agency_identifier": "097", "filters": {"fy": "2018"}}),
    )
    response_data = resp.json()
    assert len(response_data["results"]) == 2
    assert "CGAC_DOD" in response_data["results"][0]["account_name"]
    assert "CGAC_DOD" in response_data["results"][1]["account_name"]
