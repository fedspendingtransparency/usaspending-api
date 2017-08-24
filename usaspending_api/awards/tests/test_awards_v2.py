import datetime
import pytest

from rest_framework import status
from model_mommy import mommy


@pytest.mark.django_db
def test_award_last_updated_endpoint(client):
    """Test the awards endpoint."""

    test_date = datetime.datetime.now()
    test_date_reformatted = test_date.strftime('%m/%d/%Y')

    mommy.make('awards.Award', update_date=test_date)
    mommy.make('awards.Award', update_date='')

    resp = client.get('/api/v2/awards/last_updated/')
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['last_updated'] == test_date_reformatted
