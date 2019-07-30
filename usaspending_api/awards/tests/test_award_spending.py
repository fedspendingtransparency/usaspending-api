import pytest
from model_mommy import mommy
from rest_framework import status
from datetime import datetime


@pytest.fixture
def award_spending_data(db):
    ttagency = mommy.make("references.ToptierAgency")
    agency = mommy.make("references.Agency", id=111, toptier_flag=True, toptier_agency=ttagency)
    legal_entity = mommy.make("references.LegalEntity")
    award = mommy.make("awards.Award", category="grants", awarding_agency=agency)
    award1 = mommy.make("awards.Award", category="contracts", awarding_agency=agency)
    award2 = mommy.make("awards.Award", category=None, awarding_agency=agency)
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        awarding_agency=agency,
        federal_action_obligation=10,
        action_date=datetime(2017, 1, 1),
        fiscal_year=2017,
        recipient=legal_entity,
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award1,
        awarding_agency=agency,
        federal_action_obligation=20,
        action_date=datetime(2017, 9, 1),
        fiscal_year=2017,
        recipient=legal_entity,
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award1,
        awarding_agency=agency,
        federal_action_obligation=20,
        action_date=datetime(2016, 12, 1),
        fiscal_year=2017,
        recipient=legal_entity,
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award2,
        awarding_agency=agency,
        federal_action_obligation=20,
        action_date=datetime(2016, 10, 2),
        fiscal_year=2017,
        recipient=legal_entity,
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
