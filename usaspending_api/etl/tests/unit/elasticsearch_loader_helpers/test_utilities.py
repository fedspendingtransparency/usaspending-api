from usaspending_api.etl.elasticsearch_loader_helpers.utilities import is_snapshot_running


def test_is_snapshot_running(monkeypatch):
    class MockSnapshot:
        def status(self):
            return {
                "snapshots": [
                    {"snapshot": "test_snapshot", "indices": {"2021-02-12-covid19-faba": {}, "2021-02-12-awards": {}}}
                ]
            }

    class MockClient:
        snapshot = MockSnapshot()

    mock_client = MockClient()

    # snapshot running for index
    index_names = ["2021-02-12-covid19-faba"]
    result = is_snapshot_running(mock_client, index_names)
    assert result

    # snapshot not running for index
    index_names = ["2021-02-12-transactions"]
    result = is_snapshot_running(mock_client, index_names)
    assert not result

    # one of two indexes overlap with snapshot
    index_names = ["2021-02-12-awards", "2021-02-12-transactions"]
    result = is_snapshot_running(mock_client, index_names)
    assert result

    # one of two indexes overlap with snapshot (reverse order)
    index_names = ["2021-02-12-transactions", "2021-02-12-awards"]
    result = is_snapshot_running(mock_client, index_names)
    assert result
