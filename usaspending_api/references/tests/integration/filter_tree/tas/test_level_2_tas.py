from rest_framework import status

common_query = "/api/v2/references/filter_tree/tas/001/0001/?depth=0"


# Can the endpoint successfully create a search tree node?
def test_one_tas(client, basic_agency):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {
        "results": [
            {"id": "00001", "ancestors": ["001", "0001"], "description": "TAS 00001", "count": 0, "children": None}
        ]
    }


# Can the endpoint correctly populate an array?
def test_multiple_tas(client, multiple_tas):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 4


# Does the endpoint only return TAS will file D data?
def test_unsupported_tas(client, fa_with_unsupported_tas):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 0


def test_inaccurate_path(client, basic_agency):
    resp = _call_and_expect_200(client, "/api/v2/references/filter_tree/tas/002/0001/?depth=0")
    assert len(resp.json()["results"]) == 0


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
