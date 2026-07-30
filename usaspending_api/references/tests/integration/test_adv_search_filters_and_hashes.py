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


@pytest.mark.django_db
def test_request_size_limit(client):
    large_payload = {"filters": "x" * (512 * 1024 + 1)}
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=large_payload)
    assert resp.status_code == 413
    assert "exceeds maximum allowed size" in resp.content.decode()


@pytest.mark.django_db
def test_request_within_size_limit(client):
    payload_size = 512 * 1024 - 100
    valid_payload = {"filters": "x" * payload_size}
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=valid_payload)
    assert resp.status_code == status.HTTP_200_OK
    assert "hash" in resp.data


@pytest.mark.django_db
def test_hash_endpoint_with_invalid_hash_format(client):
    resp = client.post(HASH_ENDPOINT, content_type="application/json", data={"hash": "invalid_hash"})
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_hash_endpoint_with_missing_hash_key(client):
    resp = client.post(HASH_ENDPOINT, content_type="application/json", data={})
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_filter_endpoint_with_version_field(client):
    filter_payload = {"filters": {"agency": "DOT"}, "version": "2019-07-26"}
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=filter_payload)
    assert resp.status_code == status.HTTP_200_OK
    assert "hash" in resp.data


@pytest.mark.django_db
def test_filter_endpoint_with_null_filters(client):
    filter_payload = {"filters": None}
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=filter_payload)
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_filter_endpoint_with_complex_nested_filters(client):
    filter_payload = {
        "filters": {
            "keyword": ["transportation", "infrastructure"],
            "timePeriodType": "fy",
            "timePeriodFY": ["2023", "2024"],
            "selectedLocations": {
                "USA_TX": {
                    "identifier": "USA_TX",
                    "filter": {"country": "USA", "state": "TX"},
                    "display": {"entity": "State", "standalone": "TEXAS", "title": "TEXAS"}
                }
            },
            "awardType": ["A", "B", "C"]
        },
        "version": "2020-06-01"
    }
    resp = client.post(FILTER_ENDPOINT, content_type="application/json", data=filter_payload)
    assert resp.status_code == status.HTTP_200_OK
    assert "hash" in resp.data
