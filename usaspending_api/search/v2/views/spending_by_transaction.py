import copy
import logging
import re
from enum import Enum

from django.conf import settings
from django.utils.text import slugify
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import (
    InvalidParameterException,
    UnprocessableEntityException,
)
from usaspending_api.common.helpers.data_constants import state_name_from_code
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata, get_generic_filters_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.common.validator.award_filter import AWARD_FILTER_W_FILTERS
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import ToptierAgencyPublishedDABSView
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TransactionField

logger = logging.getLogger(__name__)


class DerivedField(str, Enum):
    ASSISTANCE_LISTING = "Assistance Listing"
    NAICS = "NAICS"
    PRIMARY_PLACE_OF_PERFORMANCE = "Primary Place of Performance"
    PSC = "PSC"
    RECIPIENT_LOCATION = "Recipient Location"


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction.md"

    @cache_response()
    def post(self, request):
        all_fields = [
            *[enum_val.value for enum_val in TransactionField],
            *[enum_val.value for enum_val in DerivedField],
        ]
        program_activities_rule = [
            {
                "name": "program_activities",
                "type": "array",
                "key": "filters|program_activities",
                "array_type": "object",
                "object_keys_min": 1,
                "object_keys": {
                    "name": {"type": "text", "text_type": "search"},
                    "code": {
                        "type": "integer",
                    },
                },
            }
        ]
        models = [
            {
                "name": "fields",
                "key": "fields",
                "type": "array",
                "array_type": "enum",
                "enum_values": all_fields,
                "optional": False,
            }
        ]
        models.extend(copy.deepcopy(AWARD_FILTER_W_FILTERS))
        models.extend(
            customize_pagination_with_sort_columns(
                all_fields, default_sort_column=TransactionField.TRANSACTION_AMOUNT.value
            )
        )
        models.extend(copy.deepcopy(program_activities_rule))
        self.models = models
        for m in models:
            if m["name"] in ("award_type_codes", "sort"):
                m["optional"] = False
        tiny_shield = TinyShield(models)
        validated_payload = tiny_shield.block(request.data)
        if "filters" in validated_payload and "program_activities" in validated_payload["filters"]:
            tiny_shield.enforce_object_keys_min(validated_payload, program_activities_rule[0])

        record_num = (validated_payload["page"] - 1) * validated_payload["limit"]
        if record_num >= settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW:
            raise UnprocessableEntityException(
                "Page #{page} of size {limit} is over the maximum result limit ({es_limit}). Consider using custom data downloads to obtain large data sets.".format(
                    page=validated_payload["page"],
                    limit=validated_payload["limit"],
                    es_limit=settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW,
                )
            )

        payload_sort_key = validated_payload["sort"]
        if payload_sort_key not in validated_payload["fields"]:
            raise InvalidParameterException(f"Sort value not found in fields: {payload_sort_key}")

        if "filters" in validated_payload and "no intersection" in validated_payload["filters"]["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response(
                {
                    "limit": validated_payload["limit"],
                    "results": [],
                    "page_metadata": {
                        "page": validated_payload["page"],
                        "next": None,
                        "previous": None,
                        "hasNext": False,
                        "hasPrevious": False,
                    },
                }
            )
        match payload_sort_key:
            case DerivedField.RECIPIENT_LOCATION:
                sort_by_fields = [
                    TransactionField.RECIPIENT_LOCATION_CITY_NAME.full_path,
                    TransactionField.RECIPIENT_LOCATION_STATE_CODE.full_path,
                    TransactionField.RECIPIENT_LOCATION_COUNTRY_NAME.full_path,
                    TransactionField.RECIPIENT_LOCATION_ADDRESS_LINE_1.full_path,
                    TransactionField.RECIPIENT_LOCATION_ADDRESS_LINE_2.full_path,
                    TransactionField.RECIPIENT_LOCATION_ADDRESS_LINE_3.full_path,
                ]
            case DerivedField.PRIMARY_PLACE_OF_PERFORMANCE:
                sort_by_fields = [
                    TransactionField.POP_CITY_NAME.full_path,
                    TransactionField.POP_STATE_CODE.full_path,
                    TransactionField.POP_COUNTRY_NAME.full_path,
                ]
            case DerivedField.NAICS:
                sort_by_fields = [
                    TransactionField.NAICS_CODE.full_path,
                    TransactionField.NAICS_DESCRIPTION.full_path,
                ]
            case DerivedField.PSC:
                sort_by_fields = [
                    TransactionField.PSC_CODE.full_path,
                    TransactionField.PSC_DESCRIPTION.full_path,
                ]
            case DerivedField.ASSISTANCE_LISTING:
                sort_by_fields = [
                    TransactionField.CFDA_NUMBER.full_path,
                    TransactionField.CFDA_TITLE.full_path,
                ]
            case _:
                sort_by_fields = [TransactionField(payload_sort_key).full_path]
        sorts = [{field: validated_payload["order"] for field in sort_by_fields}]

        lower_limit = (validated_payload["page"] - 1) * validated_payload["limit"]
        upper_limit = (validated_payload["page"]) * validated_payload["limit"] + 1
        if "keywords" in validated_payload["filters"]:
            validated_payload["filters"]["keyword_search"] = [
                es_minimal_sanitize(x) for x in validated_payload["filters"]["keywords"]
            ]
            validated_payload["filters"].pop("keywords")
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(validated_payload["filters"])
        search = TransactionSearch().filter(filter_query).sort(*sorts)[lower_limit:upper_limit]
        response = search.handle_execute()
        return Response(self.build_elasticsearch_result(validated_payload, response))

    def build_elasticsearch_result(self, request, response) -> dict:
        results = []
        for res in response:
            hit = res.to_dict()
            # Parsing API response values from ES query result JSON
            # We parse the `hit` (result from elasticsearch) to get the award type, use the type to determine
            # which lookup dict to use, and then use that lookup to retrieve the correct value requested from `fields`
            row = {}
            for field in request["fields"]:
                match field:
                    case DerivedField.ASSISTANCE_LISTING:
                        row[DerivedField.ASSISTANCE_LISTING.value] = {
                            "cfda_number": hit.get("cfda_number"),
                            "cfda_title": hit.get("cfda_title"),
                        }
                    case DerivedField.RECIPIENT_LOCATION:
                        row[DerivedField.RECIPIENT_LOCATION.value] = {
                            "location_country_code": hit.get("recipient_location_country_code"),
                            "country_name": hit.get("recipient_location_country_name"),
                            "state_code": hit.get("recipient_location_state_code"),
                            "state_name": state_name_from_code(hit.get("recipient_location_state_code")),
                            "city_name": hit.get("recipient_location_city_name"),
                            "county_code": hit.get("recipient_location_county_code"),
                            "county_name": hit.get("recipient_location_county_name"),
                            "address_line1": hit.get("legal_entity_address_line1"),
                            "address_line2": hit.get("legal_entity_address_line2"),
                            "address_line3": hit.get("legal_entity_address_line3"),
                            "congressional_code": hit.get("recipient_location_congressional_code"),
                            "zip4": hit.get("legal_entity_zip_last4"),
                            "zip5": hit.get("recipient_location_zip5"),
                            "foreign_postal_code": hit.get("legal_entity_foreign_posta"),
                            "foreign_province": hit.get("legal_entity_foreign_provi"),
                        }
                    case DerivedField.PRIMARY_PLACE_OF_PERFORMANCE:
                        row[DerivedField.PRIMARY_PLACE_OF_PERFORMANCE.value] = {
                            "location_country_code": hit.get("pop_country_code"),
                            "country_name": hit.get("pop_country_name"),
                            "state_code": hit.get("pop_state_code"),
                            "state_name": state_name_from_code(hit.get("pop_state_code")),
                            "city_name": hit.get("pop_city_name"),
                            "county_code": hit.get("pop_county_code"),
                            "county_name": hit.get("pop_county_name"),
                            "congressional_code": hit.get("pop_congressional_code"),
                            "zip4": hit.get("place_of_perform_zip_last4"),
                            "zip5": hit.get("pop_zip5"),
                        }
                    case DerivedField.NAICS:
                        row[DerivedField.NAICS.value] = {
                            "code": hit.get("naics_code"),
                            "description": hit.get("naics_description"),
                        }
                    case DerivedField.PSC:
                        row[DerivedField.PSC.value] = {
                            "code": hit.get("product_or_service_code"),
                            "description": hit.get("product_or_service_description"),
                        }
                    case TransactionField.AWARDING_AGENCY_SLUG | TransactionField.FUNDING_AGENCY_SLUG:
                        row[field] = slugify(hit.get(TransactionField(field).short_path))
                    case TransactionField.RECIPIENT_ID:
                        raw_value = hit.get(TransactionField.RECIPIENT_ID.short_path)
                        match_value = re.fullmatch(r"^(.*)/([CPR]{1})$", raw_value)
                        row[field] = f"{match_value[1]}-{match_value[2]}" if match_value else None
                    case _:
                        row[field] = hit.get(TransactionField(field).short_path)

            row["generated_internal_id"] = hit["generated_unique_award_id"]
            row["internal_id"] = hit["award_id"]

            results.append(row)

        metadata = get_simple_pagination_metadata(len(response), request["limit"], request["page"])

        return {
            "limit": request["limit"],
            "results": results[: request["limit"]],
            "page_metadata": metadata,
            "messages": get_generic_filters_message(request["filters"].keys(), [elem["name"] for elem in self.models]),
        }

    def get_agency_slug(self, code):
        code = str(code).zfill(3)
        submission = ToptierAgencyPublishedDABSView.objects.filter(toptier_code=code).first()
        if submission is None:
            return None
        return slugify(submission.name)
