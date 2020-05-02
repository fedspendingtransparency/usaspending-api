from rest_framework import status

base_query = "/api/v2/references/filter_tree/psc/"


def test_wrong_path_at_tier_one(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Product/B/")
    assert resp.json() == {"results": []}


def test_wrong_path_at_tier_two(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Service/A/")
    assert resp.json() == {"results": []}


def test_wrong_path_at_tier_three(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "Service/C/B5/")
    assert resp.json() == {"results": []}


def test_non_existent_group(client, basic_service):
    resp = _call_and_expect_200(client, base_query + "this_is_not_a_psc_group/")
    assert resp.json() == {"results": []}


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
