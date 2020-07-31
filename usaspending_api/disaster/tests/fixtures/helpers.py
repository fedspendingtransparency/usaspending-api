import datetime
import json
import pytest
import usaspending_api.common.helpers.fiscal_year_helpers


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
        if kwargs.get("award_type_codes"):
            filters["award_type_codes"] = kwargs["award_type_codes"]

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
    def post_for_count_endpoint(client, url, def_codes=None, award_type_codes=None):
        filters = {}

        if award_type_codes:
            filters["award_type_codes"] = award_type_codes
        if def_codes:
            filters["def_codes"] = def_codes

        request_body = json.dumps({"filter": filters})
        resp = client.post(url, content_type="application/json", data=request_body)
        return resp

    @staticmethod
    def post_for_amount_endpoint(client, url, def_codes, award_type_codes=None, award_type=None):
        filters = {}
        if def_codes:
            filters["def_codes"] = def_codes
        if award_type_codes:
            filters["award_type_codes"] = award_type_codes
        if award_type:
            filters["award_type"] = award_type
        resp = client.post(url, content_type="application/json", data=json.dumps({"filter": filters}))
        return resp

    @staticmethod
    def patch_datetime_now(monkeypatch, year, month, day):
        def patched_datetime():
            return datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)

        monkeypatch.setattr("usaspending_api.submissions.helpers.now", patched_datetime)
        monkeypatch.setattr("usaspending_api.disaster.v2.views.disaster_base.now", patched_datetime)

    @staticmethod
    def reset_dabs_cache():
        usaspending_api.disaster.v2.views.disaster_base.final_submissions_for_all_fy.cache_clear()


@pytest.fixture
def helpers():
    return Helpers
