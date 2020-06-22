import json
import pytest


class Helpers:
    @staticmethod
    def post_for_count_endpoint(client, url, def_codes=None):
        if def_codes:
            request_body = json.dumps({"filter": {"def_codes": def_codes}})
        else:
            request_body = json.dumps({"filter": {}})
        resp = client.post(url, content_type="application/json", data=request_body)
        return resp


@pytest.fixture
def helpers():
    return Helpers
