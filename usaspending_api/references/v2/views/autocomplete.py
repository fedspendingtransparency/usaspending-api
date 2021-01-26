from django.db.models import Case, F, IntegerField, Q, When
from django.db.models.functions import Upper
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Cfda, Definition, NAICS, PSC
from usaspending_api.references.v2.views.glossary import DefinitionSerializer
from usaspending_api.search.models import AgencyAutocompleteMatview


class BaseAutocompleteViewSet(APIView):
    @staticmethod
    def get_request_payload(request):
        """
        Retrieves all the request attributes needed for the autocomplete endpoints.

        Current attributes:
        * search_text : string to search for
        * limit : number of items to return
        """

        json_request = request.data

        # retrieve search_text from request
        search_text = json_request.get("search_text", None)

        try:
            limit = int(json_request.get("limit", 10))
        except ValueError:
            raise InvalidParameterException("Limit request parameter is not a valid, positive integer")

        # required query parameters were not provided
        if not search_text:
            raise InvalidParameterException("Missing one or more required request parameters: search_text")

        return search_text, limit

    # Shared autocomplete...
    def agency_autocomplete(self, request):
        """Search by subtier agencies, return those with award data, toptiers first"""

        search_text, limit = self.get_request_payload(request)

        agency_filter = Q(**{self.filter_field: True}) & (
            Q(subtier_name__icontains=search_text) | Q(subtier_abbreviation__icontains=search_text)
        )

        agencies = (
            AgencyAutocompleteMatview.objects.filter(agency_filter)
            .annotate(
                fema_sort=Case(
                    When(toptier_abbreviation="FEMA", subtier_abbreviation="FEMA", then=1),
                    When(toptier_abbreviation="FEMA", then=2),
                    default=0,
                    output_field=IntegerField(),
                )
            )
            .order_by("fema_sort", "-toptier_flag", Upper("toptier_name"), Upper("subtier_name"))
        ).values(
            "agency_autocomplete_id",
            "toptier_flag",
            "toptier_code",
            "toptier_abbreviation",
            "toptier_name",
            "subtier_abbreviation",
            "subtier_name",
        )

        results = [
            {
                "id": agency["agency_autocomplete_id"],
                "toptier_flag": agency["toptier_flag"],
                "toptier_agency": {
                    "toptier_code": agency["toptier_code"],
                    "abbreviation": agency["toptier_abbreviation"],
                    "name": agency["toptier_name"],
                },
                "subtier_agency": {"abbreviation": agency["subtier_abbreviation"], "name": agency["subtier_name"]},
            }
            for agency in agencies[:limit]
        ]

        return Response({"results": results})


class AwardingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve awarding agencies matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/awarding_agency.md"
    filter_field = "has_awarding_data"

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class FundingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve funding agencies matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/funding_agency.md"
    filter_field = "has_funding_data"

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class CFDAAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve CFDA programs matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/cfda.md"

    @cache_response()
    def post(self, request):
        """Return CFDA matches by number, title, or name"""
        search_text, limit = self.get_request_payload(request)

        queryset = Cfda.objects.all()

        # Program numbers are 10.4839, 98.2718, etc...
        if search_text.replace(".", "").isnumeric():
            queryset = queryset.filter(program_number__icontains=search_text)
        else:
            title_filter = queryset.filter(program_title__icontains=search_text)
            popular_name_filter = queryset.filter(popular_name__icontains=search_text)
            queryset = title_filter | popular_name_filter

        return Response({"results": list(queryset.values("program_number", "program_title", "popular_name")[:limit])})


class NAICSAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve NAICS objects matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/naics.md"

    @cache_response()
    def post(self, request):
        """Return all NAICS table entries matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = NAICS.objects.all()

        # NAICS codes are 111150, 112310, and there are no numeric NAICS descriptions...
        if search_text.isnumeric():
            queryset = queryset.filter(code__icontains=search_text)
        else:
            queryset = queryset.filter(description__icontains=search_text)

        # Only include 6 digit codes
        queryset = queryset.extra(where=["CHAR_LENGTH(code) = 6"])

        # rename columns...
        queryset = queryset.annotate(naics=F("code"), naics_description=F("description"))

        return Response({"results": list(queryset.values("naics", "naics_description")[:limit])})


class PSCAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve product or service (PSC) codes and their descriptions based
    on a search string. This may be the 4-character PSC code or a description string.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/psc.md"

    @cache_response()
    def post(self, request):
        """Return all PSC table entries matching the provided search text"""
        search_text, limit = self.get_request_payload(request)

        queryset = PSC.objects.all()

        # PSC codes are 4-digit, but we have some numeric PSC descriptions, so limit to 4...
        if len(search_text) == 4 and queryset.filter(code=search_text.upper()).exists():
            queryset = queryset.filter(code=search_text.upper())
        else:
            queryset = queryset.filter(description__icontains=search_text)

        # rename columns...
        queryset = queryset.annotate(product_or_service_code=F("code"), psc_description=F("description"))

        return Response({"results": list(queryset.values("product_or_service_code", "psc_description")[:limit])})


class GlossaryAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This endpoint returns glossary autocomplete data for submitted text snippet.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/glossary.md"

    @cache_response()
    def post(self, request):

        search_text, limit = self.get_request_payload(request)

        queryset = Definition.objects.all()

        glossary_terms = queryset.filter(term__icontains=search_text)[:limit]
        serializer = DefinitionSerializer(glossary_terms, many=True)

        response = {
            "search_text": search_text,
            "results": list(glossary_terms.values_list("term", flat=True)),
            "count": glossary_terms.count(),
            "matched_terms": serializer.data,
        }
        return Response(response)
