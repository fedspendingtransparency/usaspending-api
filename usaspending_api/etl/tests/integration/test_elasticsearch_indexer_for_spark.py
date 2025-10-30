import pytest

from usaspending_api.etl.elasticsearch_loader_helpers.location_dataframe import LocationDataFrame


@pytest.mark.django_db(transaction=True)
def test_location_elasticsearch_indexer(spark, elasticsearch_location_index):
    df = LocationDataFrame(spark).dataframe
    assert (
        df.filter(
            df.location.isin(
                [
                    "TEST_CITY, TEST COUNTRY",
                    "TEST_CITY_ALT, TEST COUNTRY",
                    "ANOTHER TEST CITY, TEST COUNTRY",
                    "HELLO WORLD, TEST COUNTRY",
                ]
            )
        ).count()
        == 4
    )
    assert df.filter(df.location.isin(["MO01"])).count() == 1
    response = elasticsearch_location_index.search(
        body={"query": {"multi_match": {"query": "HELLO WORLD, TEST COUNTRY", "fields": ["location"]}}},
    )
    assert response["hits"]["total"]["value"] == 1
    _ = response["hits"]["hits"][0]["_source"].pop("id")
    assert response["hits"]["hits"][0]["_source"] == {
        "location": "HELLO WORLD, TEST COUNTRY",
        "location_json": '{"city_name":"HELLO WORLD","country_name":"TEST COUNTRY","location_type":"city"}',
    }
