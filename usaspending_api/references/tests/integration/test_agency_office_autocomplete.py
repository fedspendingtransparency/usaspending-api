import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.models import AgencyAutocompleteMatview


@pytest.fixture
def agency_office_data(db):
    tn = baker.make("search.TransactionSearch", transaction_id=2, action_date="2020-01-01")

    sa = baker.make(
        "references.SubtierAgency",
        subtier_agency_id=1,
        name="ROATSSUIAR subtier",
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

    o = baker.make(
        "references.Office",
        id=1,
        agency_code="ROATSSUIAR",
        sub_tier_code="1aWERj",
        office_name="Dog House",
        office_code="11234",
    )


@pytest.mark.django_db
def test_awarding_agency_office_autocomplete_success(client, agency_office_data):
    # test for exact match
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
