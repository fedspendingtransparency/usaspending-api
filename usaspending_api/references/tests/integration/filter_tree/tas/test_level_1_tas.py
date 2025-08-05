from rest_framework import status

base_query = "/api/v2/references/filter_tree/tas/001/"
common_query = base_query + "?depth=0"


# Can the endpoint successfully create a search tree node?
def test_one_fa(client, basic_agency):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {
        "results": [
            {"id": "0001", "ancestors": ["001"], "description": "Fed Account 0001", "count": 1, "children": None}
        ]
    }


# Can the endpoint correctly populate an array?
def test_multiple_fa(client, multiple_federal_accounts):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 4


# Will the count be greater than one if there are multiple  children?
def test_multiple_children(client, fa_with_multiple_tas):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0]["count"] == 4


# Does the endpoint only return federal accounts with file D data?
def test_unsupported_fa(client, agency_with_unsupported_fa):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 0


# Does the endpoint handle depth greater than zero?
def test_positive_depth(client, multiple_federal_accounts):
    resp = _call_and_expect_200(client, base_query + "?depth=1")
    assert len(resp.json()["results"]) == 4
    # all of these should have one TAS under them
    assert len([elem["children"][0] for elem in resp.json()["results"]]) == 4


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
