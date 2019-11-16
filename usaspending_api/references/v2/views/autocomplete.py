from django.db.models import F, Q
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.constants import EXCLUDE_CGAC
from usaspending_api.references.models import Agency, Cfda, NAICS, PSC, Definition
from usaspending_api.references.v1.serializers import AgencySerializer
from usaspending_api.references.v2.views.glossary import DefinitionSerializer


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
        """Search by subtier agencies, return all, with toptiers first"""

        search_text, limit = self.get_request_payload(request)

        queryset = (
            Agency.objects.exclude(toptier_agency__toptier_code__in=EXCLUDE_CGAC)
            .filter(
                Q(subtier_agency__name__icontains=search_text) | Q(subtier_agency__abbreviation__icontains=search_text)
            )
            .order_by("-toptier_flag", "toptier_agency_id", "subtier_agency__name")
            .distinct("toptier_flag", "toptier_agency_id", "subtier_agency__name")
        )
        # The below is a one-off fix to promote FEMA as a subtier to the top when "FEMA" is searched
        # This is the only way to do this because you cannot use annotate and distinct together
        evaled = AgencySerializer(queryset[:limit], many=True).data
        evaled.sort(key=lambda x: x["toptier_agency"]["toptier_code"] == "058")
        return Response({"results": evaled})


class AwardingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve awarding agencies matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/awarding_agency.md"

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class FundingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve funding agencies matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/funding_agency.md"

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
            "results": glossary_terms.values_list("term", flat=True),
            "count": glossary_terms.count(),
            "matched_terms": serializer.data,
        }
        return Response(response)
