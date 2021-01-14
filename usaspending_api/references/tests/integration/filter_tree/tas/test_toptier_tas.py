from rest_framework import status

from usaspending_api.download.lookups import CFO_CGACS

base_query = "/api/v2/references/filter_tree/tas/"
common_query = base_query + "?depth=0"


# Can the endpoint successfully create a search tree node?
def test_one_agency(client, basic_agency):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {
        "results": [{"id": "001", "ancestors": [], "description": "Agency 001 (001)", "count": 1, "children": None}]
    }


# Can the endpoint correctly populate an array?
def test_multiple_agencies(client, cfo_agencies):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 5  # length of arbitrary_cfo_cgac_sample from fixture class


# Does the endpoint put agencies in CFO presentation order?
def test_agency_order(client, cfo_agencies, non_cfo_agencies):
    resp = _call_and_expect_200(client, common_query)
    assert (
        len(resp.json()["results"]) == 100 - len(CFO_CGACS) + 5
    )  # length of arbitrary_cfo_cgac_sample from fixture class
    assert resp.json()["results"][0]["id"] == CFO_CGACS[1]  # the first CGAC from the cfo_agencies fixture
    assert resp.json()["results"][1]["id"] == CFO_CGACS[2]  # the second CGAC from the cfo_agencies fixture
    assert resp.json()["results"][2]["id"] == CFO_CGACS[3]  # the third CGAC from the cfo_agencies fixture
    assert resp.json()["results"][3]["id"] == CFO_CGACS[7]  # the fourth CGAC from the cfo_agencies fixture
    assert resp.json()["results"][4]["id"] == CFO_CGACS[13]  # the fifth CGAC from the cfo_agencies fixture
    assert not [elem for elem in resp.json()["results"][5:] if elem["id"][:3] in CFO_CGACS]


# Does the endpoint only return agencies with file D data?
def test_unsupported_agencies(client, cfo_agencies, unsupported_agencies):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 5  # length of arbitrary_cfo_cgac_sample from fixture class


# Does the endpoint default to depth of zero?
def test_default_depth(client, cfo_agencies):
    resp = _call_and_expect_200(client, base_query)
    assert len(resp.json()["results"]) == 5  # length of arbitrary_cfo_cgac_sample from fixture class


# Does the endpoint handle depth greater than zero?
def test_positive_depth(client, cfo_agencies):
    resp = _call_and_expect_200(client, base_query + "?depth=1")
    assert len(resp.json()["results"]) == 5  # length of arbitrary_cfo_cgac_sample from fixture class
    # all of these should have one FA under them
    assert len([elem["children"][0] for elem in resp.json()["results"]]) == 5


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
