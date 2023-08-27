import pytest
from model_bakery import baker
from rest_framework import status
from datetime import datetime


@pytest.fixture
def award_spending_data(db):
    ttagency = baker.make("references.ToptierAgency", _fill_optional=True)
    agency = baker.make("references.Agency", id=111, toptier_flag=True, toptier_agency=ttagency, _fill_optional=True)
    award = baker.make("search.AwardSearch", award_id=1, category="grants", awarding_agency_id=agency.id)
    award1 = baker.make("search.AwardSearch", award_id=2, category="contracts", awarding_agency_id=agency.id)
    award2 = baker.make("search.AwardSearch", award_id=3, category=None, awarding_agency_id=agency.id)
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award=award,
        awarding_agency_id=agency.id,
        federal_action_obligation=10,
        action_date=datetime(2017, 1, 1),
        fiscal_year=2017,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award=award1,
        awarding_agency_id=agency.id,
        federal_action_obligation=20,
        action_date=datetime(2017, 9, 1),
        fiscal_year=2017,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award=award1,
        awarding_agency_id=agency.id,
        federal_action_obligation=20,
        action_date=datetime(2016, 12, 1),
        fiscal_year=2017,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award=award2,
        awarding_agency_id=agency.id,
        federal_action_obligation=20,
        action_date=datetime(2016, 10, 2),
        fiscal_year=2017,
    )


@pytest.mark.skip
@pytest.mark.django_db
def test_award_category_endpoint(client, award_spending_data):
    """Test the award_category endpoint."""

    # Test that like results are combined and results are output in descending obligated_amount order
    resp = client.get("/api/v2/award_spending/award_category/?fiscal_year=2017&awarding_agency_id=111")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 3
    assert float(resp.data["results"][0]["obligated_amount"]) == 40

    # Test for missing entries
    resp = client.get("/api/v2/award_spending/award_category/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_recipient_endpoint(client, award_spending_data):
    """Test the recipient endpoint."""

    # Test for empty optional award_category
    resp = client.get("/api/v2/award_spending/recipient/?fiscal_year=2017&awarding_agency_id=111")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 3
    assert resp.data["results"][0]["award_category"] == "contracts"

    # Test for contract optional award_category
    resp = client.get(
        "/api/v2/award_spending/recipient/?award_category=contracts&fiscal_year=2017&awarding_agency_id=111"
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["award_category"] == "contracts"
    assert float(resp.data["results"][0]["obligated_amount"]) == 40

    # Test for missing entries
    resp = client.get("/api/v2/award_spending/recipient/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
