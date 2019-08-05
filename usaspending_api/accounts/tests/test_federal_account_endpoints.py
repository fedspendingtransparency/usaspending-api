import pytest

from model_mommy import mommy


@pytest.fixture
def federal_account_models():
    fed_1 = mommy.make("accounts.FederalAccount", account_title="Community Spending", _fill_optional=True)
    fed_2 = mommy.make("accounts.FederalAccount", account_title="Military Spending", _fill_optional=True)
    mommy.make("accounts.FederalAccount", account_title="Foreign Spending", _fill_optional=True)
    mommy.make(
        "accounts.TreasuryAppropriationAccount", federal_account=fed_1, tas_rendering_label="ABC", _fill_optional=True
    )
    mommy.make(
        "accounts.TreasuryAppropriationAccount", federal_account=fed_2, tas_rendering_label="XYZ", _fill_optional=True
    )


@pytest.mark.django_db
def test_federal_account_list(federal_account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/federal_accounts/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 3
