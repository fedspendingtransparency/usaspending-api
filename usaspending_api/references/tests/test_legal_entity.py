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


@pytest.mark.django_db
def test_update_business_type_categories(recipients_data):
    le = LegalEntity.objects.filter(legal_entity_id=1111).first()

    le.business_types = 'P'
    LegalEntity.update_business_type_categories(le)
    assert le.business_categories == ['individuals']

    le.business_types = 'L'
    LegalEntity.update_business_type_categories(le)
    assert 'authorities_and_commissions' in le.business_categories
    assert 'government' in le.business_categories

    le.business_types = 'L'
    le.individual = '1'
    LegalEntity.update_business_type_categories(le)
    assert 'individuals' in le.business_categories

    le.business_types = 'L'
    le.individual = '1'
    le.city_local_government = '1'
    le.federal_agency = '1'
    LegalEntity.update_business_type_categories(le)
    assert 'national_government' in le.business_categories

    le.business_types = 'M'
    le.individual = '0'
    le.city_local_government = '1'
    le.federal_agency = '0'
    le.minority_institution = '1'
    LegalEntity.update_business_type_categories(le)
    assert 'nonprofit' in le.business_categories
    assert 'minority_serving_institution_of_higher_education' in le.business_categories
    assert 'individuals' not in le.business_categories
    assert 'national_government' not in le.business_categories
    assert 'government' in le.business_categories


@pytest.mark.django_db
def test_update_business_type_categories_faads_format(recipients_data):
    le = LegalEntity.objects.filter(legal_entity_id=1111).first()

    le.business_types = '01'  # B equivalent
    LegalEntity.update_business_type_categories(le)
    assert 'government' in le.business_categories
    assert 'local_government' in le.business_categories

    le.business_types = '12'  # M equivalent
    LegalEntity.update_business_type_categories(le)
    assert 'nonprofit' in le.business_categories

    le.business_types = '21'  # P equivalent
    LegalEntity.update_business_type_categories(le)
    assert 'individuals' in le.business_categories

    le.business_types = '23'  # R equivalent
    LegalEntity.update_business_type_categories(le)
    assert 'small_business' in le.business_categories
    assert 'category_business' in le.business_categories
