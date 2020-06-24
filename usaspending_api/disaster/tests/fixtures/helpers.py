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
    def patch_datetime_now(monkeypatch, year, month, day):
        patched_datetime = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)

        class PatchedDatetime(datetime.datetime):
            @classmethod
            def now(cls, *args):
                return patched_datetime

        monkeypatch.setattr("usaspending_api.submissions.helpers.datetime", PatchedDatetime)


@pytest.fixture
def helpers():
    return Helpers
