class MockElasticSearch:
    def __init__(self):
        pass

    def search(self,**kwargs):
        return simple_city_search()


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

stored_values = {"simple_search_by_city": simple_city_search}