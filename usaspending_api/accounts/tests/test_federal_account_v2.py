import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def fixture_data(db):
    ta0 = baker.make("references.ToptierAgency", toptier_code="001", abbreviation="ABCD", name="Dept. of Depts")
    ta1 = baker.make("references.ToptierAgency", toptier_code="002", abbreviation="EFGH", name="The Bureau")
    ta2 = baker.make("references.ToptierAgency", toptier_code="1601", abbreviation="DOL", name="Department of Labor")
    ta3 = baker.make("references.ToptierAgency", toptier_code="097", abbreviation="DOD", name="Department of Defense")
    ta4 = baker.make("references.ToptierAgency", toptier_code="021", abbreviation="DOD", name="Department of Navy")
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
    ta0 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa0)
    ta1 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)
    ta2 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa2)
    ta3 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa3)
    ta4 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa4)

    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=1000,
        submission__reporting_period_start="2017-06-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=False,  # so filter it out
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=100,
        submission__reporting_period_start="2017-03-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta0,
        total_budgetary_resources_amount_cpe=2000,
        submission__reporting_period_start="2017-06-01",
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=ta1,
        total_budgetary_resources_amount_cpe=9000,
        submission__reporting_period_start="2017-06-01",
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

    assert response_data["results"][4]["managing_agency_acronym"] == "EFGH"
    assert response_data["results"][4]["budgetary_resources"] == 9000


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
    resp = client.get("/api/v2/federal_accounts/001-0005/")

    response_data = resp.json()
    assert response_data["agency_identifier"] == "001"
    assert response_data["main_account_code"] == "0005"
    assert response_data["account_title"] == "Something"
    assert response_data["federal_account_code"] == "001-0005"
    assert response_data["parent_agency_toptier_code"] == "001"
    assert response_data["parent_agency_name"] == "Dept. of Depts"


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

    assert len(response_data["results"]) == 5
    assert "CGAC_DOD" in response_data["results"][0]["account_name"]
    assert "CGAC_DOD" in response_data["results"][1]["account_name"]
    assert "Something" in response_data["results"][2]["account_name"]
    assert "Nothing1" in response_data["results"][3]["account_name"]
    assert "Nothing2" in response_data["results"][4]["account_name"]
