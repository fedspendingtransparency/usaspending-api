import datetime
import json
import pytest


class Helpers:
    @staticmethod
    def post_for_spending_endpoint(client, url, **kwargs):
        request_body = {}
        filters = {}
        pagination = {}

        if kwargs.get("def_codes"):
            filters["def_codes"] = kwargs["def_codes"]
        if kwargs.get("query"):
            filters["query"] = kwargs["query"]

        request_body["filter"] = filters

        if kwargs.get("page"):
            pagination["page"] = kwargs["page"]
        if kwargs.get("limit"):
            pagination["limit"] = kwargs["limit"]
        if kwargs.get("order"):
            pagination["order"] = kwargs["order"]
        if kwargs.get("sort"):
            pagination["sort"] = kwargs["sort"]

        request_body["pagination"] = pagination

        if kwargs.get("spending_type"):
            request_body["spending_type"] = kwargs["spending_type"]

        resp = client.post(url, content_type="application/json", data=json.dumps(request_body))
        return resp

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
        monkeypatch.setattr("usaspending_api.disaster.v2.views.disaster_base.datetime", PatchedDatetime)


@pytest.fixture
def helpers():
    return Helpers
