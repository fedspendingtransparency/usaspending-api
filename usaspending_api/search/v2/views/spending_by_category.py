import copy
import logging

from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category as sbc_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientLookup, StateData
from usaspending_api.references.models import Agency, Cfda, LegalEntity, NAICS, PSC, RefCountryCode


logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION

ALIAS_DICT = {
    "awarding_agency": {
        "awarding_toptier_agency_name": "name",
        "awarding_subtier_agency_name": "name",
        "awarding_toptier_agency_abbreviation": "code",
        "awarding_subtier_agency_abbreviation": "code",
    },
    "funding_agency": {
        "funding_toptier_agency_name": "name",
        "funding_subtier_agency_name": "name",
        "funding_toptier_agency_abbreviation": "code",
        "funding_subtier_agency_abbreviation": "code",
    },
    "recipient_duns": {
        "recipient_id": "id",
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

ALIAS_DICT["awarding_subagency"] = ALIAS_DICT["awarding_agency"]
ALIAS_DICT["funding_subagency"] = ALIAS_DICT["funding_agency"]
ALIAS_DICT["recipient_parent_duns"] = ALIAS_DICT["recipient_duns"]


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """

    @cache_response()
    def post(self, request: dict):
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
            self.queryset = sbc_view_queryset(self.category, self.filters)
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
        if self.category in ("awarding_agency", "awarding_subagency"):
            results = self.awarding_agency()
        elif self.category in ("funding_agency", "funding_subagency"):
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
        }
        return response

    def awarding_agency(self) -> list:
        if self.category == "awarding_agency":
            filters = {"awarding_toptier_agency_name__isnull": False}
            values = ["awarding_toptier_agency_name", "awarding_toptier_agency_abbreviation"]
        elif self.category == "awarding_subagency":
            filters = {"awarding_subtier_agency_name__isnull": False}
            values = ["awarding_subtier_agency_name", "awarding_subtier_agency_abbreviation"]

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])
        results = alias_response(ALIAS_DICT[self.category], query_results)
        for row in results:
            row["id"] = fetch_agency_tier_id_by_agency(row["name"], self.category == "awarding_subagency")
        return results

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
            # Not implemented until Parent Recipient Name's in LegalEntity and matviews
            self.raise_not_implemented()
            # filters = {'parent_recipient_unique_id__isnull': False}
            # values = ['recipient_name', 'parent_recipient_unique_id']

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])
        for row in query_results:
            row["recipient_id"] = None
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


def fetch_agency_tier_id_by_agency(agency_name, is_subtier=False):
    agency_type = "subtier_agency" if is_subtier else "toptier_agency"
    columns = ["id"]
    filters = {"{}__name".format(agency_type): agency_name}
    if not is_subtier:
        # Note: The awarded/funded subagency can be a toptier agency, so we don't filter only subtiers in that case.
        filters["toptier_flag"] = True
    result = Agency.objects.filter(**filters).values(*columns).first()
    if not result:
        logger.warning("{} not found for agency_name: {}".format(",".join(columns), agency_name))
        return None
    return result[columns[0]]


def fetch_recipient_id_by_duns(duns):
    columns = ["legal_entity_id"]
    result = LegalEntity.objects.filter(recipient_unique_id=duns).values(*columns).first()
    if not result:
        logger.warning("{} not found for duns: {}".format(",".join(columns), duns))
        return None
    return result[columns[0]]


def fetch_cfda_id_title_by_number(cfda_number):
    columns = ["id", "program_title"]
    result = Cfda.objects.filter(program_number=cfda_number).values(*columns).first()
    if not result:
        logger.warning("{} not found for cfda_number: {}".format(",".join(columns), cfda_number))
        return None, None
    return result[columns[0]], result[columns[1]]


def fetch_psc_description_by_code(psc_code):
    columns = ["description"]
    result = PSC.objects.filter(code=psc_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for psc_code: {}".format(",".join(columns), psc_code))
        return None
    return result[columns[0]]


def fetch_country_name_from_code(country_code):
    columns = ["country_name"]
    result = RefCountryCode.objects.filter(country_code=country_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for country_code: {}".format(",".join(columns), country_code))
        return None
    return result[columns[0]]


def fetch_state_name_from_code(state_code):
    columns = ["name"]
    result = StateData.objects.filter(code=state_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for state_code: {}".format(",".join(columns), state_code))
        return None
    return result[columns[0]]


def fetch_naics_description_from_code(naics_code, passthrough=None):
    columns = ["description"]
    result = NAICS.objects.filter(code=naics_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for naics_code: {}".format(",".join(columns), naics_code))
        return passthrough
    return result[columns[0]]
