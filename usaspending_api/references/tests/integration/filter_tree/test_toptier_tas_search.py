from rest_framework import status
from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.constants import DOD_SUBSUMED_CGAC, DHS_SUBSUMED_CGAC
from usaspending_api.references.tests.integration.filter_tree.tas_data_fixtures import aribitrary_cfo_cgac_sample

common_query = "/api/v2/references/filter_tree/tas/?depth=0"


# Can the endpoint successfully create a search tree node?
def test_one_agency(client, basic_agencies, basic_tas):
    resp = client.get(common_query)
    assert resp.json() == {
        "results": [{"id": "001", "ancestors": [], "description": "Agency 001", "count": 0, "children": None}]
    }


# Do agencies NOT appear when there is no matching TAS?
def test_no_tas(client, basic_agencies):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {"results": []}


# Do the agencies sort in desired order?
def test_sorted_agencies(client, one_tas_per_agency):
    resp = _call_and_expect_200(client, common_query)
    ids = [elem["description"] for elem in resp.json()["results"]]  # ensure that this test fails for only one reason
    assert ids == [
        f"CFO Agency {str(id).zfill(3)}" for id in [CFO_CGACS[i] for i in sorted(aribitrary_cfo_cgac_sample)]
    ] + [
        f"Agency {str(id).zfill(3)}"
        for id in range(1, 100)
        if str(id).zfill(3) not in CFO_CGACS
        and str(id).zfill(3) not in DOD_SUBSUMED_CGAC
        and str(id).zfill(3) not in DHS_SUBSUMED_CGAC
    ]


# Do the other armed forces correctly roll up into the DoD?
def test_dod_subsuming_logic(client, tas_for_dod_subs):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 1


# Does the DoD roll up into the DoD?
def test_dod_still_fetches_dod(client, tas_for_dod):
    resp = _call_and_expect_200(client, common_query)
    assert len(resp.json()["results"]) == 1


# If a TAS has a matching FREC, does it find that FREC agency?
def test_frec_matching(client, tas_for_frec):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {
        "results": [{"id": "0001", "ancestors": [], "description": "FREC Agency 0001", "count": 0, "children": None}]
    }


# Do CGAC and FREC agencies work together?
def test_cgac_and_frec_matching(client, basic_tas, tas_for_frec):
    resp = _call_and_expect_200(client, common_query)
    print(resp.json())
    assert resp.json() == {
        "results": [
            {"id": "001", "ancestors": [], "description": "Agency 001", "count": 0, "children": None},
            {"id": "0001", "ancestors": [], "description": "FREC Agency 0001", "count": 0, "children": None},
        ]
    }


# If a TAS has a FREC, but it doesn't match, does it fall back to CGAC?
def test_frec_fallback_functionality(client, frec_tas_with_no_match):
    resp = _call_and_expect_200(client, common_query)
    assert resp.json() == {
        "results": [{"id": "001", "ancestors": [], "description": "Agency 001", "count": 0, "children": None}]
    }


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
