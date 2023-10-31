import json

import pytest
from model_bakery import baker
from rest_framework import status


from usaspending_api.references.models import Definition


@pytest.fixture
def glossary_data(db):
    baker.make(Definition, term="Word", plain="Plaintext response.", official="Official language.")
    baker.make(Definition, term="Word2", plain="Plaintext response. 2", official="Official language. 2")
    baker.make(Definition, term="Word3", plain="Plaintext response. 3", official="Official language. 3")


@pytest.mark.django_db
def test_glossary_v2_autocomplete(client):
    baker.make(Definition, term="Abacus", slug="ab")
    baker.make(Definition, term="Aardvark", slug="aa")

    resp = client.post(
        "/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({"search_text": "ab"})
    )
    json_response = json.loads(resp.content.decode("utf-8"))
    assert resp.status_code == status.HTTP_200_OK
    assert json_response["search_text"] == "ab"
    assert json_response["results"] == ["Abacus"]

    resp = client.post(
        "/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({"search_text": "aa"})
    )
    json_response = json.loads(resp.content.decode("utf-8"))
    assert resp.status_code == status.HTTP_200_OK
    assert json_response["search_text"] == "aa"
    assert json_response["results"] == ["Aardvark"]

    resp = client.post(
        "/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({"search_text": "a"})
    )
    json_response = json.loads(resp.content.decode("utf-8"))
    assert resp.status_code == status.HTTP_200_OK
    assert json_response["search_text"] == "a"
    assert sorted(json_response["results"]) == ["Aardvark", "Abacus"]

    resp = client.post(
        "/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({"search_text": "b"})
    )
    json_response = json.loads(resp.content.decode("utf-8"))
    assert resp.status_code == status.HTTP_200_OK
    assert json_response["search_text"] == "b"
    assert sorted(json_response["results"]) == ["Abacus"]

    resp = client.post(
        "/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({"search_text": "aab"})
    )
    json_response = json.loads(resp.content.decode("utf-8"))
    assert resp.status_code == status.HTTP_200_OK
    assert json_response["search_text"] == "aab"
    assert sorted(json_response["results"]) == []


@pytest.mark.django_db
def test_bad_v2_glossary_autocomplete_request(client):
    """Verify error on bad autocomplete request for awards."""

    resp = client.post("/api/v2/autocomplete/glossary/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
