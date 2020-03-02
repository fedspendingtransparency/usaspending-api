from rest_framework import status


def test_one_agency(client, basic_agencies, basic_tas):
    resp = client.get("/api/v2/references/filter_tree/tas/?depth=0")

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == [{"id": "001", "ancestors": [], "description": "Agency 001", "count": 0, "children": None}]
