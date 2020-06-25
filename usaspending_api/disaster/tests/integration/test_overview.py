import pytest

OVERVIEW_URL = "/api/v2/disaster/overview/"


@pytest.mark.django_db
def test_empty_data_set(client, basic_gtas, basic_faba):
    resp = client.get(OVERVIEW_URL)
    print(resp.data)
    assert False
