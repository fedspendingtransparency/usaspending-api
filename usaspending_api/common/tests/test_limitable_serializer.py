import pytest
import json

from model_mommy import mommy

from usaspending_api.awards.models import Award


@pytest.fixture
def mock_limitable_data():
    mommy.make(Award, _fill_optional=True)


@pytest.mark.django_db
def test_nested_field_limiting(client, mock_limitable_data):
    request_object = {"fields": ["piid", "recipient__recipient_name"]}

    response = client.post(
        "/api/v1/awards/", content_type="application/json", data=json.dumps(request_object), format="json"
    )

    results = response.data["results"][0]

    assert "piid" in results.keys()
    assert "recipient" in results.keys()
    assert "recipient_name" in results.get("recipient", {}).keys()


@pytest.mark.django_db
def test_nested_field_exclusion(client, mock_limitable_data):
    request_object = {"exclude": ["piid", "recipient__recipient_name"]}

    response = client.post(
        "/api/v1/awards/", content_type="application/json", data=json.dumps(request_object), format="json"
    )

    results = response.data["results"][0]

    assert "piid" not in results.keys()
    assert "recipient" in results.keys()
    assert "recipient_name" not in results.get("recipient").keys()
