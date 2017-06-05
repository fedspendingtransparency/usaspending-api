import pytest
from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def award_spending_data(db):
    agency = mommy.make('references.Agency', id=111)
    legal_entity = mommy.make('references.LegalEntity')
    award = mommy.make('awards.Award', category='grants', awarding_agency=agency)
    mommy.make(
        'awards.Transaction',
        award=award,
        awarding_agency=agency,
        federal_action_obligation=10,
        fiscal_year=2017,
        recipient=legal_entity
    )
    mommy.make(
        'awards.Transaction',
        award=award,
        awarding_agency=agency,
        federal_action_obligation=20,
        fiscal_year=2017,
        recipient=legal_entity
    )


@pytest.mark.django_db
def test_award_type_endpoint(client, award_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/award_spending/award_type/?fiscal_year=2017&awarding_agency_id=111')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) > 0

    # TODO: Separate this check into a unit test
    assert float(resp.data['results'][0]['obligated_amount']) == 30

    resp = client.get('/api/v2/award_spending/award_type/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_recipient_endpoint(client, award_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/award_spending/recipient/?fiscal_year=2017&awarding_agency_id=111')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) > 0

    # TODO: Separate this check into a unit test
    assert float(resp.data['results'][0]['obligated_amount']) == 30

    resp = client.get('/api/v2/award_spending/recipient/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
