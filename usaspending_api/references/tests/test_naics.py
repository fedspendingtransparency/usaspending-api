from model_mommy import mommy
import pytest
import json


@pytest.fixture()
def naics_test_data():
    mommy.make("references.NAICS", code="11", description="Agriculture, Forestry, Fishing and Hunting")
    mommy.make("references.NAICS", code="1111", description="Oilseed and Grain Farming")
    mommy.make("references.NAICS", code="111110", description="Soybean Farming")
    mommy.make("references.NAICS", code="111120", description="Oilseed (except Soybean) Farming")
    mommy.make("references.NAICS", code="1112", description="Vegetable and Melon Farming")
    mommy.make("references.NAICS", code="111211", description="Potato Farming")
    mommy.make("references.NAICS", code="21", description="Mining, Quarrying, and Oil and Gas Extraction")
    mommy.make("references.NAICS", code="22", description="Utilities")


@pytest.mark.django_db
def test_default(client, naics_test_data):

    resp = client.get("/api/v2/references/naics/")  # get all tier 1 codes
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 3
    expected_data = [
        {"naics": "11", "naics_description": "Agriculture, Forestry, Fishing and Hunting", "count": 3},
        {"naics": "21", "naics_description": "Mining, Quarrying, and Oil and Gas Extraction", "count": 0},
        {"naics": "22", "naics_description": "Utilities", "count": 0},
    ]
    assert resp.data["results"] == expected_data


@pytest.mark.django_db
def test_with_id(client, naics_test_data):

    resp = client.get("/api/v2/references/naics/11/")
    assert resp.status_code == 200
    expected_data = [
        {
            "naics": "11",
            "naics_description": "Agriculture, Forestry, Fishing and Hunting",
            "count": 3,
            "children": [
                {"naics": "1111", "naics_description": "Oilseed and Grain Farming", "count": 2},
                {"naics": "1112", "naics_description": "Vegetable and Melon Farming", "count": 1},
            ],
        }
    ]
    assert resp.data["results"] == expected_data

    resp = client.get("/api/v2/references/naics/1111/")
    assert resp.status_code == 200
    expected_data = [
        {
            "naics": "1111",
            "naics_description": "Oilseed and Grain Farming",
            "count": 2,
            "children": [
                {"naics": "111110", "naics_description": "Soybean Farming", "count": 1},
                {"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1},
            ],
        }
    ]
    assert resp.data["results"] == expected_data

    resp = client.get("/api/v2/references/naics/111120/")
    assert resp.status_code == 200
    expected_data = [{"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1}]
    assert resp.data["results"] == expected_data

    resp = client.get("/api/v2/references/naics/1/")
    assert resp.status_code == 200
    expected_data = []
    assert resp.data["results"] == expected_data


@pytest.mark.django_db
def test_with_filter(client, naics_test_data):

    resp = client.get("/api/v2/references/naics/?filter=fish")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data

    resp = client.get("/api/v2/references/naics/?filter=grain")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [
                    {"naics": "1111", "naics_description": "Oilseed and Grain Farming", "count": 2, "children": []}
                ],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data

    resp = client.get("/api/v2/references/naics/?filter=soybean")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [
                    {
                        "naics": "1111",
                        "naics_description": "Oilseed and Grain Farming",
                        "count": 2,
                        "children": [
                            {"naics": "111110", "naics_description": "Soybean Farming", "count": 1},
                            {"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1},
                        ],
                    }
                ],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data

    resp = client.get("/api/v2/references/naics/?filter=farming")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [
                    {
                        "naics": "1111",
                        "naics_description": "Oilseed and Grain Farming",
                        "count": 2,
                        "children": [
                            {"naics": "111110", "naics_description": "Soybean Farming", "count": 1},
                            {"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1},
                        ],
                    },
                    {
                        "naics": "1112",
                        "naics_description": "Vegetable and Melon Farming",
                        "count": 1,
                        "children": [{"naics": "111211", "naics_description": "Potato Farming", "count": 1}],
                    },
                ],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data

    resp = client.get("/api/v2/references/naics/?filter=b")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [
                    {
                        "naics": "1111",
                        "naics_description": "Oilseed and Grain Farming",
                        "count": 2,
                        "children": [
                            {"naics": "111110", "naics_description": "Soybean Farming", "count": 1},
                            {"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1},
                        ],
                    },
                    {"naics": "1112", "naics_description": "Vegetable and Melon Farming", "count": 1, "children": []},
                ],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data


@pytest.mark.django_db
def test_filter_with_code(client, naics_test_data):
    resp = client.get("/api/v2/references/naics/11/?filter=Oilseed")
    assert resp.status_code == 200
    expected_data = {
        "results": [
            {
                "naics": "11",
                "naics_description": "Agriculture, Forestry, Fishing and Hunting",
                "count": 3,
                "children": [
                    {
                        "naics": "1111",
                        "naics_description": "Oilseed and Grain Farming",
                        "count": 2,
                        "children": [
                            {"naics": "111120", "naics_description": "Oilseed (except Soybean) Farming", "count": 1},
                        ],
                    }
                ],
            }
        ]
    }
    assert json.loads(resp.content.decode("utf-8")) == expected_data