import json
import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import Rosetta


TEST_JSON_DOC = json.dumps({"headers": ["brats", "burgers"], "data": {"rows": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}})


@pytest.fixture
def rosetta_data(db):
    baker.make(Rosetta, document_name="api_response", document=TEST_JSON_DOC)


@pytest.mark.django_db
def test_rosetta_endpoint(client, rosetta_data):
    """test partial-text search on awards."""

    resp = client.get("/api/v2/references/data_dictionary/")

    assert resp.status_code == status.HTTP_200_OK, "Expected HTTP 200"
    assert resp.data["document"] == TEST_JSON_DOC, "Document obtained from API doesn't match DB document"
