from rest_framework import status
from usaspending_api.references.tests.integration.filter_tree.psc.psc_data_fixtures import (
    rnd_tier_two,
    rnd_tier_three,
    rnd_tier_four,
    product_tier_two,
    product_tier_three,
    service_tier_two,
    service_tier_three,
    service_tier_four,
)

base_query = "/api/v2/references/filter_tree/psc/"


def test_toptier_psc(client, no_data):
    resp = _call_and_expect_200(client, base_query)
    assert len(resp.json()["results"]) == 3


def test_tier_two_rnd(client, basic_rnd):
    resp = _call_and_expect_200(client, base_query + "Research%20and%20Development/")
    assert resp.json() == {"results": [rnd_tier_two()]}


def test_tier_three_rnd(client, basic_rnd):
    resp = _call_and_expect_200(client, base_query + "Research%20and%20Development/AA/")
    assert resp.json() == {"results": [rnd_tier_three()]}


def test_tier_four_rnd(client, basic_rnd):
    resp = _call_and_expect_200(client, base_query + "Research%20and%20Development/AA/AA9/")
    assert resp.json() == {"results": [rnd_tier_four()]}


def test_tier_two_product(client, basic_product):
    resp = _call_and_expect_200(client, base_query + "Product/")
    assert resp.json() == {"results": [product_tier_two()]}


def test_tier_three_product(client, basic_product):
    resp = _call_and_expect_200(client, base_query + "Product/10/")
    assert resp.json() == {"results": [product_tier_three()]}


def test_tier_two_service(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Service/")
    assert resp.json() == {"results": [service_tier_two()]}


def test_tier_three_service(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Service/B/")
    assert resp.json() == {"results": [service_tier_three()]}


def test_tier_four_service(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Service/B/B5/")
    assert resp.json() == {"results": [service_tier_four()]}


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
