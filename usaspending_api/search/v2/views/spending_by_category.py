import copy
import logging

from django.conf import settings
from django.db.models import Case, IntegerField, Sum, Value, When
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata, get_time_period_message
from usaspending_api.common.recipient_lookups import combine_recipient_hash_and_level
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_agency_tier_id_by_agency,
    fetch_cfda_id_title_by_number,
    fetch_psc_description_by_code,
    fetch_naics_description_from_code,
    fetch_country_name_from_code,
    fetch_state_name_from_code,
)
from usaspending_api.search.v2.views.spending_by_category_views.awarding_agency import AwardingAgencyViewSet
from usaspending_api.search.v2.views.spending_by_category_views.awarding_subagency import AwardingSubagencyViewSet

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION

ALIAS_DICT = {
    "funding_agency": {
        "funding_toptier_agency_name": "name",
        "funding_subtier_agency_name": "name",
        "funding_toptier_agency_abbreviation": "code",
        "funding_subtier_agency_abbreviation": "code",
    },
    "recipient_duns": {
        "recipient_id": "recipient_id",
        "recipient_name": "name",
        "recipient_unique_id": "code",
        "parent_recipient_unique_id": "code",
    },
    "cfda": {
        "cfda_number": "code",
        # Note: we could pull cfda title from the matviews but noticed the titles vary for the same cfda number
        #       which leads to incorrect groupings
        # 'cfda_title': 'name'
    },
    "psc": {"product_or_service_code": "code"},
    "naics": {"naics_code": "code", "naics_description": "name"},
    "county": {"pop_county_code": "code", "pop_county_name": "name"},
    "district": {"pop_congressional_code": "code"},
    "state_territory": {"pop_state_code": "code"},
    "country": {"pop_country_code": "code"},
    "federal_account": {"federal_account_id": "id", "federal_account_display": "code", "account_title": "name"},
}

ALIAS_DICT["funding_subagency"] = ALIAS_DICT["funding_agency"]
ALIAS_DICT["recipient_parent_duns"] = ALIAS_DICT["recipient_duns"]


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        """Return all budget function/subfunction titles matching the provided search text"""
        categories = [
            "awarding_agency",
            "awarding_subagency",
            "funding_agency",
            "funding_subagency",
            "recipient_duns",
            "recipient_parent_duns",
            "cfda",
            "psc",
            "naics",
            "county",
            "district",
            "country",
            "state_territory",
            "federal_account",
        ]
        models = [
            {"name": "category", "key": "category", "type": "enum", "enum_values": categories, "optional": False},
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False, "optional": True},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        # Apply/enforce POST body schema and data validation in request
        validated_payload = TinyShield(models).block(request.data)

        validated_payload["elasticsearch"] = is_experimental_elasticsearch_api(request)

        if validated_payload["category"] == "awarding_agency":
            return Response(AwardingAgencyViewSet().perform_search(validated_payload))
        elif validated_payload["category"] == "awarding_subagency":
            return Response(AwardingSubagencyViewSet().perform_search(validated_payload))

        # Execute the business logic for the endpoint and return a python dict to be converted to a Django response
        return Response(BusinessLogic(validated_payload).results())


class BusinessLogic:
    # __slots__ will keep this object smaller
    __slots__ = (
        "subawards",
        "category",
        "page",
        "limit",
        "obligation_column",
        "lower_limit",
        "upper_limit",
        "filters",
        "queryset",
    )

    def __init__(self, payload: dict):
        """
            payload is tightly integrated with
        """
        self.subawards = payload["subawards"]
        self.category = payload["category"]
        self.page = payload["page"]
        self.limit = payload["limit"]
        self.filters = payload.get("filters", {})

        self.lower_limit = (self.page - 1) * self.limit
        self.upper_limit = self.page * self.limit + 1  # Add 1 for simple "Next Page" check

        if self.subawards:
            self.queryset = subaward_filter(self.filters)
            self.obligation_column = "amount"
        else:
            self.queryset = spending_by_category_view_queryset(self.category, self.filters)
            self.obligation_column = "generated_pragmatic_obligation"

    def raise_not_implemented(self):
        msg = "Category '{}' is not implemented"
        if self.subawards:
            msg += " when `subawards` is True"
        raise InvalidParameterException(msg.format(self.category))

    def common_db_query(self, filters, values):
        return (
            self.queryset.filter(**filters)
            .values(*values)
            .annotate(amount=Sum(self.obligation_column))
            .order_by("-amount")
        )

    def results(self) -> dict:
        results = []
        # filter the transactions by category
        if self.category in ("funding_agency", "funding_subagency"):
            results = self.funding_agency()
        elif self.category in ("recipient_duns", "recipient_parent_duns"):
            results = self.recipient()
        elif self.category in ("cfda", "psc", "naics"):
            results = self.industry_and_other_codes()
        elif self.category in ("county", "district", "state_territory", "country"):
            results = self.location()
        elif self.category in ("federal_account"):
            results = self.federal_account()

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            "category": self.category,
            "limit": self.limit,
            "page_metadata": page_metadata,
            # alias_response is a workaround for tests instead of applying any aliases in the querysets
            "results": results[: self.limit],
            "messages": [get_time_period_message()],
        }
        return response

    def funding_agency(self) -> list:
        """
        NOTICE: These categories were originally included for both Prime and Sub awards.
        However, there are concerns with the data sparsity so it is unlikely to be used immediately.
        So this category will be "dark" for now (removed from API doc). If undesired long-term they should be
        removed from code and API contract.
        """
        if self.category == "funding_agency":
            filters = {"funding_toptier_agency_name__isnull": False}
            values = ["funding_toptier_agency_name", "funding_toptier_agency_abbreviation"]
        elif self.category == "funding_subagency":
            filters = {"funding_subtier_agency_name__isnull": False}
            values = ["funding_subtier_agency_name", "funding_subtier_agency_abbreviation"]

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])

        results = alias_response(ALIAS_DICT[self.category], query_results)
        for row in results:
            row["id"] = fetch_agency_tier_id_by_agency(row["name"], self.category == "funding_subagency")
        return results

    @staticmethod
    def _get_recipient_id(row):
        """
        In the recipient_profile table there is a 1 to 1 relationship between hashes and DUNS
        (recipient_unique_id) and the hashes+duns match exactly between recipient_profile and
        recipient_lookup where there are matches.  Grab the level from recipient_profile by
        hash if we have one or by DUNS if we have one of those.
        """
        if "recipient_hash" in row:
            profile_filter = {"recipient_hash": row["recipient_hash"]}
        elif "recipient_unique_id" in row:
            profile_filter = {"recipient_unique_id": row["recipient_unique_id"]}
        else:
            raise RuntimeError(
                "Attempted to lookup recipient profile using a queryset that contains neither "
                "'recipient_hash' nor 'recipient_unique_id'"
            )

        profile = (
            RecipientProfile.objects.filter(**profile_filter)
            .exclude(recipient_name__in=SPECIAL_CASES)
            .annotate(
                sort_order=Case(
                    When(recipient_level="C", then=Value(0)),
                    When(recipient_level="R", then=Value(1)),
                    default=Value(2),
                    output_field=IntegerField(),
                )
            )
            .values("recipient_hash", "recipient_level")
            .order_by("sort_order")
            .first()
        )

        return (
            combine_recipient_hash_and_level(profile["recipient_hash"], profile["recipient_level"]) if profile else None
        )

    def recipient(self) -> list:
        if self.category == "recipient_duns":
            filters = {}
            if self.subawards:
                values = ["recipient_name", "recipient_unique_id"]
            else:
                values = ["recipient_hash"]

        elif self.category == "recipient_parent_duns":
            # TODO: check if we can aggregate on recipient name and parent duns,
            #    since parent recipient name isn't available
            # Not implemented until "Parent Recipient Name" is in matviews
            self.raise_not_implemented()
            # filters = {'parent_recipient_unique_id__isnull': False}
            # values = ['recipient_name', 'parent_recipient_unique_id']

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])
        for row in query_results:

            row["recipient_id"] = self._get_recipient_id(row)

            if not self.subawards:

                lookup = (
                    RecipientLookup.objects.filter(recipient_hash=row["recipient_hash"])
                    .values("legal_business_name", "duns")
                    .first()
                )

                # The Recipient Name + DUNS should always be retrievable in RecipientLookup
                # For odd edge cases or data sync issues, handle gracefully:
                if lookup is None:
                    lookup = {}

                row["recipient_name"] = lookup.get("legal_business_name", None)
                row["recipient_unique_id"] = lookup.get("duns", "DUNS Number not provided")
                del row["recipient_hash"]

        results = alias_response(ALIAS_DICT[self.category], query_results)
        return results

    def industry_and_other_codes(self) -> list:
        if self.category == "cfda":
            filters = {"{}__isnull".format(self.obligation_column): False, "cfda_number__isnull": False}
            values = ["cfda_number"]
        elif self.category == "psc":
            if self.subawards:
                # N/A for subawards
                self.raise_not_implemented()
            filters = {"product_or_service_code__isnull": False}
            values = ["product_or_service_code"]
        elif self.category == "naics":
            if self.subawards:
                # TODO: get subaward NAICS from Broker
                self.raise_not_implemented()
            filters = {"naics_code__isnull": False}
            values = ["naics_code", "naics_description"]

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])

        results = alias_response(ALIAS_DICT[self.category], query_results)
        for row in results:
            if self.category == "cfda":
                row["id"], row["name"] = fetch_cfda_id_title_by_number(row["code"])
            elif self.category == "psc":
                row["id"] = None
                row["name"] = fetch_psc_description_by_code(row["code"])
            elif self.category == "naics":
                row["id"] = None
                row["name"] = fetch_naics_description_from_code(row["code"], row["name"])
        return results

    def location(self) -> list:
        filters = {}
        values = {}
        if self.category == "county":
            filters = {"pop_county_code__isnull": False}
            values = ["pop_county_code", "pop_county_name"]
        elif self.category == "district":
            filters = {"pop_congressional_code__isnull": False}
            values = ["pop_congressional_code", "pop_state_code"]
        elif self.category == "country":
            filters = {"pop_country_code__isnull": False}
            values = ["pop_country_code"]
        elif self.category == "state_territory":
            filters = {"pop_state_code__isnull": False}
            values = ["pop_state_code"]

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])

        results = alias_response(ALIAS_DICT[self.category], query_results)
        for row in results:
            row["id"] = None
            if self.category == "district":
                cd_code = row["code"]
                if cd_code == "90":  # 90 = multiple districts
                    cd_code = "MULTIPLE DISTRICTS"

                row["name"] = "{}-{}".format(row["pop_state_code"], cd_code)
                del row["pop_state_code"]
            if self.category == "country":
                row["name"] = fetch_country_name_from_code(row["code"])
            if self.category == "state_territory":
                row["name"] = fetch_state_name_from_code(row["code"])
        return results

    def federal_account(self) -> list:
        # Awards -> FinancialAccountsByAwards -> TreasuryAppropriationAccount -> FederalAccount
        filters = {"federal_account_id__isnull": False}
        values = ["federal_account_id", "federal_account_display", "account_title"]

        if self.subawards:
            # N/A for subawards
            self.raise_not_implemented()

        # Note: For performance reasons, limiting to only recipient profile requests
        if "recipient_id" not in self.filters:
            raise InvalidParameterException("Federal Account category requires recipient_id in search filter")

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])
        return alias_response(ALIAS_DICT[self.category], query_results)
