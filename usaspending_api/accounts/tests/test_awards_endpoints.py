import pytest

from model_mommy import mommy
import json


@pytest.fixture
def award_models():
    fed_acct = mommy.make("accounts.FederalAccount", agency_identifier="084")
    treas_acct1 = mommy.make(
        "accounts.TreasuryAppropriationAccount", federal_account=fed_acct, budget_function_title="Commemorative Plaques"
    )
    treas_acct2 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct,
        budget_function_title="Forbidden Ancient Secrets",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards", treasury_account=treas_acct1, drv_obligations_incurred_total_by_award=1000
    )
    mommy.make(
        "awards.FinancialAccountsByAwards", treasury_account=treas_acct1, drv_obligations_incurred_total_by_award=2000
    )
    mommy.make(
        "awards.FinancialAccountsByAwards", treasury_account=treas_acct2, drv_obligations_incurred_total_by_award=50000
    )


@pytest.mark.django_db
def test_awards_list(award_models, client):
    """
    Ensure the accounts/awards/ endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/accounts/awards/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 3


@pytest.mark.django_db
def test_awards(award_models, client):
    """
    Ensure the accounts/awards/total/ aggregation counts properly
    """

    response_tas_sums = {"Commemorative Plaques": "3000.00", "Forbidden Ancient Secrets": "50000.00"}

    resp = client.post(
        "/api/v1/accounts/awards/total/",
        content_type="application/json",
        data=json.dumps(
            {"field": "drv_obligations_incurred_total_by_award", "group": "treasury_account__budget_function_title"}
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_tas_sums[result["item"]] == result["aggregate"]
