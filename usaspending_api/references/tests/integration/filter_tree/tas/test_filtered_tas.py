from rest_framework import status

base_query = "/api/v2/references/filter_tree/tas/"


def test_basic_matching_toptier(client, basic_agency):
    resp = _call_and_expect_200(client, base_query + "?filter=agency")
    assert resp.json() == {
        "results": [{"id": "001", "ancestors": [], "description": "Agency 001 (001)", "count": 1, "children": None}]
    }


def test_basic_nonmatching_toptier(client, basic_agency):
    resp = _call_and_expect_200(client, base_query + "?filter=wrong")
    assert resp.json() == {"results": []}


def test_matching_on_fa(client, basic_agency):
    resp = _call_and_expect_200(client, base_query + "?depth=1&filter=0001")
    assert resp.json() == {
        "results": [
            {
                "id": "001",
                "ancestors": [],
                "description": "Agency 001 (001)",
                "count": 1,
                "children": [
                    {
                        "id": "0001",
                        "ancestors": ["001"],
                        "description": "Fed Account 0001",
                        "count": 1,
                        "children": None,
                    }
                ],
            }
        ]
    }


def test_matching_on_tas(client, basic_agency):
    resp = _call_and_expect_200(client, base_query + "?depth=2&filter=00001")
    assert resp.json() == {
        "results": [
            {
                "id": "001",
                "ancestors": [],
                "description": "Agency 001 (001)",
                "count": 1,
                "children": [
                    {
                        "id": "0001",
                        "ancestors": ["001"],
                        "description": "Fed Account 0001",
                        "count": 1,
                        "children": [
                            {
                                "id": "00001",
                                "ancestors": ["001", "0001"],
                                "description": "TAS 00001",
                                "count": 0,
                                "children": None,
                            }
                        ],
                    }
                ],
            }
        ]
    }


def _call_and_expect_200(client, url):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    return resp
