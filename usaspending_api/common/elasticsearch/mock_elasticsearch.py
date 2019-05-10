class MockElasticSearch:
    def __init__(self):
        pass

    def search(self):
        return {
    "took": 46,
    "timed_out": false,
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

