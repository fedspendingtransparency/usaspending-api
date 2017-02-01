import pytest

from model_mommy import mommy


@pytest.fixture(scope="session")
def fin_act_models():
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        _quantity=2)


@pytest.mark.django_db
def test_financial_activities_list(fin_act_models, client):
    """
    Ensure the financial activities endpoint lists the right number of entities
    """
    resp = client.get('/api/v1/financial_activities/')
    assert resp.status_code == 200
    assert len(resp.data) == 3
