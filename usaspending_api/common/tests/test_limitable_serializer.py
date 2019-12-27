import pytest
import json

from model_mommy import mommy

from usaspending_api.awards.models import Award


@pytest.fixture
def mock_limitable_data():
    mommy.make(Award, _fill_optional=True)


@pytest.mark.django_db
def test_nested_field_limiting(client, mock_limitable_data):
    request_object = {"fields": ["piid", "awarding_agency__toptier_agency__abbreviation"]}

    response = client.post(
        "/api/v1/awards/", content_type="application/json", data=json.dumps(request_object), format="json"
    )

    results = response.data["results"][0]

    assert "piid" in results.keys()
    assert "awarding_agency" in results.keys()
    assert "abbreviation" in results.get("awarding_agency", {}).get("toptier_agency", {}).keys()


@pytest.mark.django_db
def test_nested_field_exclusion(client, mock_limitable_data):
    request_object = {"exclude": ["piid", "awarding_agency__toptier_agency__abbreviation"]}

    response = client.post(
        "/api/v1/awards/", content_type="application/json", data=json.dumps(request_object), format="json"
    )

    results = response.data["results"][0]

    assert "piid" not in results.keys()
    assert "awarding_agency" in results.keys()
    assert "abbreviation" not in results.get("awarding_agency", {}).get("toptier_agency", {}).keys()
