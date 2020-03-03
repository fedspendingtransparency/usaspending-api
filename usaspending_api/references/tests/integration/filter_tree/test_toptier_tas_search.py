from rest_framework import status
from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.constants import DOD_SUBSUMED_CGAC
from usaspending_api.references.tests.integration.filter_tree.tas_data_fixtures import aribitrary_cfo_cgac_sample


def test_one_agency(client, basic_agencies, basic_tas):
    resp = client.get("/api/v2/references/filter_tree/tas/?depth=0")

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == [{"id": "001", "ancestors": [], "description": "Agency 001", "count": 0, "children": None}]


def test_sorted_agencies(client, one_tas_per_agency):
    resp = client.get("/api/v2/references/filter_tree/tas/?depth=0")

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    ids = [elem["description"] for elem in resp.json()]  # ensure that this test fails for only one reason
    assert ids == [
        f"CFO Agency {str(id).zfill(3)}" for id in [CFO_CGACS[i] for i in sorted(aribitrary_cfo_cgac_sample)]
    ] + [
        f"Agency {str(id).zfill(3)}"
        for id in range(1, 100)
        if str(id).zfill(3) not in CFO_CGACS and str(id).zfill(3) not in DOD_SUBSUMED_CGAC
    ]


def test_dod_subsuming_logic(client, tas_for_dod_subs):
    resp = client.get("/api/v2/references/filter_tree/tas/?depth=0")

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(resp.json()) == 3


def test_dod_still_fetches_did(client, tas_for_dod):
    resp = client.get("/api/v2/references/filter_tree/tas/?depth=0")

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(resp.json()) == 1
