from collections import OrderedDict
from typing import Any

from django.db.models import QuerySet
from django.template import loader
from rest_framework.pagination import BasePagination
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.urls import replace_query_param
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException


class UsaspendingPagination(BasePagination):
    # The default page size
    page_size = 100

    # The maximum page size
    max_page_size = 500

    # Keeps "page * limit" comfortably inside a Postgres bigint for OFFSET.
    max_page = 2**31 - 1

    # Page size query param
    page_size_query_param = "limit"

    # Page query param
    page_query_param = "page"

    # Use a lazy template (i.e. doesn't need to know the total number of pages)
    template = "rest_framework/pagination/previous_and_next.html"

    def paginate_queryset(self, queryset: QuerySet, request: Request, view: APIView | None = None) -> list:
        self.request = request
        self.count = queryset.count()
        self.limit = self.get_limit(request)
        self.page = self.get_page(request)
        self.offset = self.get_offset(request)
        self.has_next_page = self.next_page_exists(queryset)
        self.has_previous_page = bool(self.page > 1)

        # Turn on the controls if we have multiple pages
        if self.next_page_exists and self.template is not None:
            self.display_page_controls = True

        return list(queryset[self.offset : self.offset + self.limit])

    def next_page_exists(self, queryset: QuerySet) -> bool:
        # If there are more results than the number of records returned so far, return True
        return (self.offset + self.limit) < self.count

    @staticmethod
    def _as_int(value: object, name: str) -> int:
        # Values arrive as a list from GET query params and as a scalar from a JSON body.
        if isinstance(value, list):
            value = value[0] if value else None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise InvalidParameterException(f"'{name}' must be an integer, got {value!r}") from None

    def get_limit(self, request: Request) -> int:
        # Check both POST and GET parameters for limit
        request_parameters = {**request.data, **request.query_params}

        if "limit" in request_parameters:
            lim = self._as_int(request_parameters["limit"], "limit")
            # This will ensure our limit is bounded by [1, max_page_size]
            return max(min(lim, self.max_page_size), 1)
        else:
            return self.page_size

    def get_page(self, request: Request) -> int:
        # Check both POST and GET parameters for page
        request_parameters = {**request.data, **request.query_params}
        if "page" in request_parameters:
            p = self._as_int(request_parameters["page"], "page")
            if p > self.max_page:
                raise InvalidParameterException(f"'page' must be at most {self.max_page}")
            # Ensures our page is bounded by [1, max_page]
            return max(p, 1)
        else:
            return 1

    def get_offset(self, request: Request) -> int:
        return (self.page - 1) * self.limit

    def get_paginated_response(self, data: Any) -> Response:
        page_metadata = OrderedDict(
            [
                ("count", self.count),
                ("page", self.page),
                ("has_next_page", self.has_next_page),
                ("has_previous_page", self.has_previous_page),
                ("next", self.get_next_link()),
                ("current", self.get_current_link()),
                ("previous", self.get_previous_link()),
            ]
        )

        return Response(OrderedDict([("page_metadata", page_metadata), ("results", data)]))

    def get_next_link(self) -> str | None:
        if not self.has_next_page:
            return None

        url = self.request.build_absolute_uri()
        url = replace_query_param(url, self.page_size_query_param, self.limit)
        url = replace_query_param(url, self.page_query_param, self.page + 1)

        return url

    def get_current_link(self) -> str:
        url = self.request.build_absolute_uri()
        url = replace_query_param(url, self.page_size_query_param, self.limit)
        url = replace_query_param(url, self.page_query_param, self.page)

        return url

    def get_previous_link(self) -> str | None:
        if self.page == 1:
            return None

        url = self.request.build_absolute_uri()
        url = replace_query_param(url, self.page_size_query_param, self.limit)
        url = replace_query_param(url, self.page_query_param, self.page - 1)

        return url

    def get_html_context(self) -> dict:
        return {"previous_url": self.get_previous_link(), "next_url": self.get_next_link()}

    def to_html(self) -> str:
        template = loader.get_template(self.template)
        context = self.get_html_context()
        return template.render(context)
