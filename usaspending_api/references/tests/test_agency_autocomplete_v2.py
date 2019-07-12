import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import Agency


@pytest.fixture
def agency_data(db):
    mommy.make(
        Agency,
        toptier_agency__name="Lunar Colonization Society",
        toptier_agency__cgac_code="LCS123",
        subtier_agency=None,
        _fill_optional=True,
    ),
    mommy.make(
        Agency,
        toptier_agency__name="Cerean Mineral Extraction Corp.",
        toptier_agency__cgac_code="CMEC",
        subtier_agency=None,
        _fill_optional=True,
    ),
    mommy.make(
        Agency,
        toptier_agency__name="Department of Transportation",
        subtier_agency__name="Department of Transportation",
        toptier_flag=True,
        _fill_optional=True,
    )
    mommy.make(
        Agency,
        toptier_agency__name="Department of Defence",
        subtier_agency__name="Department of the Army",
        subtier_agency__abbreviation="USA",
        toptier_flag=False,
        _fill_optional=True,
    )


@pytest.mark.django_db
def test_awarding_agency_autocomplete_success(client, agency_data):

    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "Department of Transportation"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"

    # test for similar matches
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "department", "limit": 3}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # test toptier match at top
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"
    assert resp.data["results"][1]["subtier_agency"]["name"] == "Department of the Army"


@pytest.mark.django_db
def test_awarding_agency_autocomplete_failure(client):
    """Verify error on bad autocomplete request for awarding agency."""

    resp = client.post("/api/v2/autocomplete/awarding_agency/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_funding_agency_autocomplete_success(client, agency_data):

    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/funding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "Department of Transportation"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"

    # test for similar matches
    resp = client.post(
        "/api/v2/autocomplete/funding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "department", "limit": 3}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # test toptier match at top
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"
    assert resp.data["results"][1]["subtier_agency"]["name"] == "Department of the Army"


@pytest.mark.django_db
def test_funding_agency_autocomplete_failure(client):
    """Empty string test"""
    resp = client.post("/api/v2/autocomplete/funding_agency/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_awarding_agency_autocomplete_by_abbrev(client, agency_data):
    "Verify that search on subtier abbreviation works"

    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "USA"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of the Army"

    # test for failure
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data=json.dumps({"search_text": "ABC"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0
