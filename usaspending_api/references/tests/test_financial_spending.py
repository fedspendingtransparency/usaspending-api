import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def award_spending_data(db):
    agency1 = mommy.make('references.Agency', id=111)
    awd1 = mommy.make('awards.Award', category='grants', awarding_agency=agency1)
    mommy.make(
        'awards.Transaction',
        award=awd1,
        awarding_agency=agency1,
        federal_action_obligation=10,
        fiscal_year=2017
    )


@pytest.mark.django_db
def test_award_type_endpoint(client, award_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/financial_spending/object_class/?fiscal_year=2017&agency_id=111')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) > 0

    resp = client.get('/api/v2/financial_spending/object_class/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
