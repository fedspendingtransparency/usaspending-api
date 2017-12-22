import pytest
import json

from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data():
    le = mommy.make(
        LegalEntity,
        legal_entity_id=1111,
        recipient_name="Lunar Colonization Society",
        recipient_unique_id="LCS123")
    # Model Mommy doesn't like setting ArrayField at instantiation
    LegalEntity.objects.filter(pk=le.pk).update(business_categories=["us_government_entity", "minority_owned_business"])


@pytest.mark.django_db
def test_endpoints(client, recipients_data):
    resp = client.get("/api/v1/references/recipients/")
    assert resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v1/references/recipients/1111/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["recipient_unique_id"] == "LCS123"
    assert len(resp.data["business_categories"]) == 2

    resp = client.post("/api/v1/references/recipients/",
                       content_type='application/json',
                       data=json.dumps({
                            "filters": [
                                {
                                    "field": "business_categories",
                                    "operation": "contains",
                                    "value": "us_government_entity"
                                }
                            ]
                       }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post("/api/v1/references/recipients/",
                       content_type='application/json',
                       data=json.dumps({
                            "filters": [
                                {
                                    "field": "business_categories",
                                    "operation": "contains",
                                    "value": ["us_government_entity"]
                                }
                            ]
                       }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post("/api/v1/references/recipients/",
                       content_type='application/json',
                       data=json.dumps({
                            "filters": [
                                {
                                    "field": "business_categories",
                                    "operation": "contains",
                                    "value": ["us_government_entity", "minority_owned_business"]
                                }
                            ]
                       }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post("/api/v1/references/recipients/",
                       content_type='application/json',
                       data=json.dumps({
                            "filters": [
                                {
                                    "field": "business_categories",
                                    "operation": "contains",
                                    "value": ["us_government_entity", "nonprofit"]
                                }
                            ]
                       }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0
