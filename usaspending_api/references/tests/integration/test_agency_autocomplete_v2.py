import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.models import AgencyAutocompleteMatview


@pytest.fixture
def agency_data(db):
    tn = baker.make("search.TransactionSearch", transaction_id=1, action_date="1900-01-01")

    a = baker.make(
        "references.Agency",
        id=1,
        toptier_agency__name="Really Old Agency That Shouldn't Show Up In Any Results",
        toptier_agency__toptier_code="ROATSSUIAR",
        subtier_agency__name="ROATSSUIAR subtier",
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=1,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )

    tn = baker.make("search.TransactionSearch", transaction_id=2, action_date="2020-01-01")

    a = baker.make(
        "references.Agency",
        id=2,
        toptier_agency__name="Agency With No Subtier That Shouldn't Show Up In Any Results",
        toptier_agency__toptier_code="AWNSTSSUIAR",
        subtier_agency=None,
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )

    a = baker.make(
        "references.Agency",
        id=3,
        toptier_agency__name="Lunar Colonization Society",
        toptier_agency__toptier_code="LCS123",
        subtier_agency__name="Darkside Chapter",
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )

    a = baker.make(
        "references.Agency",
        id=4,
        toptier_agency__name="Cerean Mineral Extraction Corp.",
        toptier_agency__toptier_code="CMEC",
        subtier_agency__name="Copper Division",
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=4,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )

    a = baker.make(
        "references.Agency",
        id=5,
        toptier_agency__name="Department of Transportation",
        subtier_agency__name="Department of Transportation",
        toptier_flag=True,
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=5,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )

    a = baker.make(
        "references.Agency",
        id=6,
        toptier_agency__name="Department of Defense",
        subtier_agency__name="Department of the Army",
        subtier_agency__abbreviation="USA",
        toptier_flag=False,
        _fill_optional=True,
    )
    baker.make(
        "search.AwardSearch",
        award_id=6,
        awarding_agency_id=a.id,
        funding_agency_id=a.id,
        latest_transaction_id=tn.transaction_id,
        certified_date=tn.action_date,
    )


@pytest.mark.django_db
def test_awarding_agency_autocomplete_success(client, agency_data):
    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data={"search_text": "Department of Transportation"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"

    # test for similar matches
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/",
        content_type="application/json",
        data={"search_text": "department", "limit": 3},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # test toptier match at top
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"
    assert resp.data["results"][1]["subtier_agency"]["name"] == "Department of the Army"


@pytest.mark.django_db
def test_awarding_agency_autocomplete_failure(client):
    """Verify error on bad autocomplete request for awarding agency."""

    resp = client.post("/api/v2/autocomplete/awarding_agency/", content_type="application/json", data={})
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_funding_agency_autocomplete_success(client, agency_data):
    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/funding_agency/",
        content_type="application/json",
        data={"search_text": "Department of Transportation"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of Transportation"

    # test for similar matches
    resp = client.post(
        "/api/v2/autocomplete/funding_agency/",
        content_type="application/json",
        data={"search_text": "department", "limit": 3},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # test toptier match at top
    assert sorted(
        [resp.data["results"][0]["subtier_agency"]["name"], resp.data["results"][1]["subtier_agency"]["name"]]
    ) == ["Department of Transportation", "Department of the Army"]


@pytest.mark.django_db
def test_funding_agency_autocomplete_failure(client):
    """Empty string test"""
    resp = client.post("/api/v2/autocomplete/funding_agency/", content_type="application/json", data={})
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_awarding_agency_autocomplete_by_abbrev(client, agency_data):
    """Verify that search on subtier abbreviation works"""

    # test for exact match
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/", content_type="application/json", data={"search_text": "USA"}
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["subtier_agency"]["name"] == "Department of the Army"

    # test for failure
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/", content_type="application/json", data={"search_text": "ABC"}
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0


@pytest.mark.django_db
def test_for_bogus_agencies(client, agency_data):
    """Ensure our bogus agencies do not show up in results."""

    resp = client.post(
        "/api/v2/autocomplete/awarding_agency/", content_type="application/json", data={"search_text": "Results"}
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0

    # Just double check that our bogus agencies are not in the materialized view.
    assert AgencyAutocompleteMatview.objects.filter(pk=1).count() == 0
    assert AgencyAutocompleteMatview.objects.filter(pk=2).count() == 0
    assert AgencyAutocompleteMatview.objects.count() == 4
