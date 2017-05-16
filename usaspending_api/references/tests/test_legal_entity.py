import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data(db):
    mommy.make(
        LegalEntity,
        legal_entity_id=1111,
        recipient_name="Lunar Colonization Society",
        recipient_unique_id="LCS123")


@pytest.mark.django_db
def test_get_or_create_by_duns():
    # Null duns should always return True for created
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True

    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True

    # Let's create a new record with a DUNS
    entity, created = LegalEntity.get_or_create_by_duns(duns="123456789")
    assert created
    assert LegalEntity.get_or_create_by_duns(duns="123456789")[0] == entity
    assert not LegalEntity.get_or_create_by_duns(duns="123456789")[1]


@pytest.mark.django_db
def test_endpoints(client, recipients_data):
    resp = client.get("/api/v1/references/recipients/")
    assert resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v1/references/recipients/1111/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["recipient_unique_id"] == "LCS123"
