from elasticsearch import ConnectionError


class MockElasticSearch:
    behavior = "default"
    calls = 0
    def __init__(self,behavior):
        self.behavior = behavior

    def search(self,**kwargs):
        self.calls += 1
        return stored_values[self.behavior]()


def simple_city_search():
    return {
        "took": 46,
        "timed_out": False,
        "_shards": {
            "total": 5,
            "successful": 5,
            "skipped": 0,
            "failed": 0
        },
        "hits": {
            "total": 34800,
            "max_score": 4.362885,
            "hits": [
                {
                    "_index": "city",
                    "_type": "transaction_mapping",
                    "_id": "wixPf2oBxLxzmuVlwtl3",
                    "_score": 4.362885,
                    "_source": {
                        "recipient_location_state_code": "NY",
                        "recipient_location_city_name": "CORTLANDT MANOR"
                    }
                }
            ]
        }
    }


def connection_error():
    return ConnectionError


stored_values = {"simple_search_by_city": simple_city_search, "connection_error": connection_error}
