import datetime
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

    @staticmethod
    def patch_date_today(monkeypatch, year, month, day):
        patched_today = datetime.date(year, month, day)

        class PatchedDate:
            @classmethod
            def today(cls):
                return patched_today

        monkeypatch.setattr("usaspending_api.submissions.helpers.date", PatchedDate)


@pytest.fixture
def helpers():
    return Helpers
