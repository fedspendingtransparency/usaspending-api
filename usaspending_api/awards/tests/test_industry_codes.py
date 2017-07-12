import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def industry_codes_data():

    # CREATE obligated_amount(s)
    amount1 = mommy.make('awards.Transaction', federal_action_obligation=20.00)
    amount2 = mommy.make('awards.Transaction', federal_action_obligation=10.00)
    amount3 = mommy.make('awards.Transaction', federal_action_obligation=10.00)
    amount4 = mommy.make('awards.Transaction', federal_action_obligation=5.00)

    # CREATE PSC - NAICS relationships
    mommy.make(
        'awards.TransactionContract',
        product_or_service_code='A123',
        naics='123456',
        naics_description='Cleaning Services',
        transaction=amount1,
    )
    mommy.make(
        'awards.TransactionContract',
        product_or_service_code='B456',
        naics='456789',
        naics_description='Cooking Services',
        transaction=amount2
    )
    mommy.make(
        'awards.TransactionContract',
        product_or_service_code='A123',
        naics='456789',
        naics_description='Cooking Services',
        transaction=amount3,
    )
    mommy.make(
        'awards.TransactionContract',
        product_or_service_code='A123',
        naics='456789',
        naics_description='Cooking Services',
        transaction=amount4
    )


@pytest.mark.django_db
def test_industry_codes(client, industry_codes_data):
    """
        Test the industry_codes endpoint.
        Test obligated amounts sorted from high to low.
        Test obligated amounts sum for group2.
    """
    resp = client.get('/api/v2/industry_codes/?fiscal_year=2017')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3
    group1 = resp.data['results'][0]
    assert group1['psc'] == 'A123'
    assert group1['naics'] == '123456'
    assert group1['description'] == 'Cleaning Services'
    assert group1['obligated_amount'] == '20.00'
    group2 = resp.data['results'][1]
    assert group2['psc'] == 'A123'
    assert group2['naics'] == '456789'
    assert group2['description'] == 'Cooking Services'
    assert group2['obligated_amount'] == '15.00'
    group3 = resp.data['results'][2]
    assert group3['psc'] == 'B456'
    assert group3['naics'] == '456789'
    assert group3['description'] == 'Cooking Services'
    assert group3['obligated_amount'] == '10.00'


@pytest.mark.django_db
def test_industry_codes_params(client, industry_codes_data):
    """Test for bad request due to missing params."""
    resp = client.get('/api/v2/industry_codes/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
