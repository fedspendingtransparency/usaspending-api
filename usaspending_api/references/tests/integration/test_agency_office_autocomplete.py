import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def agency_office_data(db):
    tn = baker.make("search.TransactionSearch", transaction_id=1, action_date="2020-01-01")

    sa = baker.make(
        "references.SubtierAgency",
        subtier_agency_id=1,
        name="ROATSSUIAR",
        subtier_code="1aWERj",
        abbreviation="abc",
        _fill_optional=True,
    )

    ta = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=1,
        name="Really Old Agency That Shouldn't Show Up In Any Results",
        toptier_code="ROATSSUIAR",
        _fill_optional=True,
    )

    a = baker.make(
        "references.Agency",
        id=1,
        toptier_agency_id=ta.toptier_agency_id,
        subtier_agency_id=sa.subtier_agency_id,
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

    baker.make(
        "references.Office",
        id=1,
        agency_code="ROATSSUIAR",
        sub_tier_code="1aWERj",
        office_name="Dog House",
        office_code="11234",
    )

    tn = baker.make("search.TransactionSearch", transaction_id=2, action_date="2020-01-01")

    sa = baker.make(
        "references.SubtierAgency",
        subtier_agency_id=2,
        name="Blahblah",
        subtier_code="32adsf",
        abbreviation="ads",
        _fill_optional=True,
    )

    ta = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2,
        name="Department of Defense",
        toptier_code="ASDF",
        _fill_optional=True,
    )

    a = baker.make(
        "references.Agency",
        id=2,
        toptier_agency_id=ta.toptier_agency_id,
        subtier_agency_id=sa.subtier_agency_id,
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

    tn = baker.make("search.TransactionSearch", transaction_id=3, action_date="2020-01-01")

    sa = baker.make(
        "references.SubtierAgency",
        subtier_agency_id=3,
        name="Match all three",
        subtier_code="hasdf",
        abbreviation="asdfh",
        _fill_optional=True,
    )

    ta = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=3,
        name="toptier_agency_id is 3",
        toptier_code="asdhg",
        abbreviation="Match all three",
        _fill_optional=True,
    )

    a = baker.make(
        "references.Agency",
        id=3,
        toptier_agency_id=ta.toptier_agency_id,
        subtier_agency_id=sa.subtier_agency_id,
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

    baker.make(
        "references.Office",
        id=3,
        agency_code="asdhg",
        sub_tier_code="hasdf",
        office_name="Match all three",
        office_code="12111",
    )


@pytest.mark.django_db
def test_awarding_agency_office_autocomplete_success(client, agency_office_data):
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency_office/",
        content_type="application/json",
        data={"search_text": "Really Old Agency That Shouldn't Show Up In Any Results"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 0
    assert len(resp.data["results"]["office"]) == 0
    assert (
        resp.data["results"]["toptier_agency"][0]["name"] == "Really Old Agency That Shouldn't Show Up In Any Results"
    )
    assert resp.data["results"]["toptier_agency"][0]["code"] == "ROATSSUIAR"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1

    # Test for when search text matches a toptier with no office
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency_office/",
        content_type="application/json",
        data={"search_text": "Department of Defense"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 0
    assert len(resp.data["results"]["office"]) == 0
    assert resp.data["results"]["toptier_agency"][0]["name"] == "Department of Defense"
    assert resp.data["results"]["toptier_agency"][0]["code"] == "ASDF"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 0

    # Test toptier agency, subtier agency, and office match search text
    # Test for when search text matches a toptier with no office
    resp = client.post(
        "/api/v2/autocomplete/awarding_agency_office/",
        content_type="application/json",
        data={"search_text": "Match all three"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 1
    assert len(resp.data["results"]["office"]) == 1

    # Response schema validation
    assert set(["subtier_agency", "toptier_agency", "code", "name"]) == set(resp.data["results"]["office"][0].keys())
    assert set(["toptier_agency", "offices", "abbreviation", "name", "code"]) == set(
        resp.data["results"]["subtier_agency"][0].keys()
    )
    assert set(["subtier_agencies", "offices", "abbreviation", "name", "code"]) == set(
        resp.data["results"]["toptier_agency"][0].keys()
    )

    # Toptier agency object validation
    assert resp.data["results"]["toptier_agency"][0]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["toptier_agency"][0]["abbreviation"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["code"] == "asdhg"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1

    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert resp.data["results"]["toptier_agency"][0]["subtier_agencies"][0]["name"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["subtier_agencies"][0]["code"] == "hasdf"

    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1
    assert resp.data["results"]["toptier_agency"][0]["offices"][0]["name"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["offices"][0]["code"] == "12111"

    # Subtier agency object validation
    assert resp.data["results"]["subtier_agency"][0]["name"] == "Match all three"
    assert resp.data["results"]["subtier_agency"][0]["code"] == "hasdf"
    assert resp.data["results"]["subtier_agency"][0]["toptier_agency"]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["subtier_agency"][0]["toptier_agency"]["code"] == "asdhg"
    assert len(resp.data["results"]["subtier_agency"][0]["offices"]) == 1
    assert resp.data["results"]["subtier_agency"][0]["offices"][0]["name"] == "Match all three"
    assert resp.data["results"]["subtier_agency"][0]["offices"][0]["code"] == "12111"

    # Office object validation
    assert resp.data["results"]["office"][0]["name"] == "Match all three"
    assert resp.data["results"]["office"][0]["code"] == "12111"

    assert resp.data["results"]["office"][0]["toptier_agency"]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["office"][0]["toptier_agency"]["code"] == "asdhg"

    assert resp.data["results"]["office"][0]["subtier_agency"]["name"] == "Match all three"
    assert resp.data["results"]["office"][0]["subtier_agency"]["code"] == "hasdf"


@pytest.mark.django_db
def test_funding_agency_office_autocomplete_success(client, agency_office_data):
    resp = client.post(
        "/api/v2/autocomplete/funding_agency_office/",
        content_type="application/json",
        data={"search_text": "Really Old Agency That Shouldn't Show Up In Any Results"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 0
    assert len(resp.data["results"]["office"]) == 0
    assert (
        resp.data["results"]["toptier_agency"][0]["name"] == "Really Old Agency That Shouldn't Show Up In Any Results"
    )
    assert resp.data["results"]["toptier_agency"][0]["code"] == "ROATSSUIAR"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1

    # Test for when search text matches a toptier with no office
    resp = client.post(
        "/api/v2/autocomplete/funding_agency_office/",
        content_type="application/json",
        data={"search_text": "Department of Defense"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 0
    assert len(resp.data["results"]["office"]) == 0
    assert resp.data["results"]["toptier_agency"][0]["name"] == "Department of Defense"
    assert resp.data["results"]["toptier_agency"][0]["code"] == "ASDF"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 0

    # Test toptier agency, subtier agency, and office match search text
    # Test for when search text matches a toptier with no office
    resp = client.post(
        "/api/v2/autocomplete/funding_agency_office/",
        content_type="application/json",
        data={"search_text": "Match all three"},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["toptier_agency"]) == 1
    assert len(resp.data["results"]["subtier_agency"]) == 1
    assert len(resp.data["results"]["office"]) == 1

    # Response schema validation
    assert set(["subtier_agency", "toptier_agency", "code", "name"]) == set(resp.data["results"]["office"][0].keys())
    assert set(["toptier_agency", "offices", "abbreviation", "name", "code"]) == set(
        resp.data["results"]["subtier_agency"][0].keys()
    )
    assert set(["subtier_agencies", "offices", "abbreviation", "name", "code"]) == set(
        resp.data["results"]["toptier_agency"][0].keys()
    )

    # Toptier agency object validation
    assert resp.data["results"]["toptier_agency"][0]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["toptier_agency"][0]["abbreviation"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["code"] == "asdhg"
    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1

    assert len(resp.data["results"]["toptier_agency"][0]["subtier_agencies"]) == 1
    assert resp.data["results"]["toptier_agency"][0]["subtier_agencies"][0]["name"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["subtier_agencies"][0]["code"] == "hasdf"

    assert len(resp.data["results"]["toptier_agency"][0]["offices"]) == 1
    assert resp.data["results"]["toptier_agency"][0]["offices"][0]["name"] == "Match all three"
    assert resp.data["results"]["toptier_agency"][0]["offices"][0]["code"] == "12111"

    # Subtier agency object validation
    assert resp.data["results"]["subtier_agency"][0]["name"] == "Match all three"
    assert resp.data["results"]["subtier_agency"][0]["code"] == "hasdf"
    assert resp.data["results"]["subtier_agency"][0]["toptier_agency"]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["subtier_agency"][0]["toptier_agency"]["code"] == "asdhg"
    assert len(resp.data["results"]["subtier_agency"][0]["offices"]) == 1
    assert resp.data["results"]["subtier_agency"][0]["offices"][0]["name"] == "Match all three"
    assert resp.data["results"]["subtier_agency"][0]["offices"][0]["code"] == "12111"

    # Office object validation
    assert resp.data["results"]["office"][0]["name"] == "Match all three"
    assert resp.data["results"]["office"][0]["code"] == "12111"

    assert resp.data["results"]["office"][0]["toptier_agency"]["name"] == "toptier_agency_id is 3"
    assert resp.data["results"]["office"][0]["toptier_agency"]["code"] == "asdhg"

    assert resp.data["results"]["office"][0]["subtier_agency"]["name"] == "Match all three"
    assert resp.data["results"]["office"][0]["subtier_agency"]["code"] == "hasdf"
