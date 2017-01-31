import pytest

from model_mommy import mommy


@pytest.fixture(scope="session")
def account_models():
    mommy.make('accounts.AppropriationAccountBalances', _quantity=2)


@pytest.mark.django_db
def test_account_list(account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get('/api/v1/accounts/')
    assert resp.status_code == 200
    assert len(resp.data['results']) == 2


@pytest.mark.django_db
def test_tas_list(account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get('/api/v1/accounts/tas/')
    assert resp.status_code == 200
    assert len(resp.data) == 3
