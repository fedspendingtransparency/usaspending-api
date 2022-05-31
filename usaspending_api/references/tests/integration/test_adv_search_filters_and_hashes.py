import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import FilterHash


HASH_ENDPOINT = "/api/v2/references/hash/"
FILTER_ENDPOINT = "/api/v2/references/filter/"


@pytest.fixture
def stored_hashes(db):
    baker.make("references.FilterHash", filter={}, hash="")


@pytest.mark.django_db
def test_missing_hash(client):
    resp = client.post(
        HASH_ENDPOINT, content_type="application/json", data={"hash": "1c89eccf09b7dc74a75b651af79602e7"}
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_generate_hash_success(client):
    resp = client.post(
        FILTER_ENDPOINT, content_type="application/json", data={"filters": "Department of Transportation"}
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["hash"] == "1c89eccf09b7dc74a75b651af79602e7"


@pytest.mark.django_db
def test_new_hash(client):
    filter_payload = {"filters": "Department of Transportation"}
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=filter_payload)

    resp = client.post(
        HASH_ENDPOINT, content_type="application/json", data={"hash": "1c89eccf09b7dc74a75b651af79602e7"}
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["filter"] == filter_payload


@pytest.mark.django_db
def test_hash_algorithm(client):
    import hashlib
    import json

    filter_payloads = [
        {"filters": "Department of Transportation"},
        {"filters": {"agency": {"name": "Department of Transportation"}}},
        {"filters": {"agency": {"name": "DOT", "level": "toptier"}}},
        {"filters": {"def_codes": ["A", "B", "C", "9"], "cfda": ["10.987", "19.001"]}},
        {"filters": {"agency": {"name": "Department of Transportation"}}},
        {"empty": None},
    ]

    def get_hash_from_api(payload):
        return client.post(FILTER_ENDPOINT, content_type="application/json", data=payload).data["hash"]

    def hash_payload(payload):
        m = hashlib.md5()
        m.update(json.dumps(payload).encode("utf8"))
        return str(m.hexdigest().encode("utf8"))[2:-1]

    def get_filters_from_db(provided_hash):
        return FilterHash.objects.get(hash=provided_hash).filter

    for fp in filter_payloads:
        assert get_hash_from_api(fp) == hash_payload(fp)
        assert fp == get_filters_from_db(hash_payload(fp))
