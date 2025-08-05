from django.db.models import Case, F, IntegerField, Q, When
from django.db.models.functions import Upper
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import deprecated_api_endpoint_message
from usaspending_api.references.models import NAICS, PSC, Cfda, Definition, RefProgramActivity
from usaspending_api.references.v2.views.glossary import DefinitionSerializer
from usaspending_api.search.models import AgencyAutocompleteMatview, AgencyOfficeAutocompleteMatview


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

        results = {"results": results}
        messages = results.get("messages", [])
        deprecated_api_endpoint_message(messages)
        results["messages"] = messages
        return Response(results)

    def agency_office_autocomplete(self, request):
        """Returns a collection of agencies, sub-agencies, and offices that match the request."""

        search_text, limit = self.get_request_payload(request)
        # It's important to order by toptier fields so that results are deterministic between objects.
        # For that reason we define one set of order by arguments to be re-used throughout this method.
        order_by = ["-toptier_flag", Upper("toptier_name"), Upper("subtier_name")]

        # Begin deriving toptier agency objects for response
        toptier_agency_filter = Q(**{self.filter_field: True}) & (
            Q(toptier_abbreviation__icontains=search_text) | Q(toptier_name__icontains=search_text)
        )

        toptier_agency_search_text_matches = (
            AgencyOfficeAutocompleteMatview.objects.filter(toptier_agency_filter).order_by(*order_by).values().all()
        )

        toptier_agency_tracker = {}
        subtiers_already_added = []
        for toptier_agency in toptier_agency_search_text_matches:
            # This key is created so that we can treat multiple records with the same
            # toptier values as a single result
            key = f'{toptier_agency["toptier_abbreviation"]}{toptier_agency["toptier_code"]}{toptier_agency["toptier_name"]}'
            if key not in toptier_agency_tracker:
                toptier_agency_tracker[key] = {}
                toptier_result = self._agency_office_toptier_agency_response_object(toptier_agency)
                toptier_agency_tracker[key] = toptier_result
                # This list is reset so that we can track which subtiers have already
                # been added to the results for the distinct pair of toptier values of the current iteration.
                # It's needed because our mat view is at the office grain.
                subtiers_already_added = []
            if "subtier_agencies" not in toptier_agency_tracker[key]:
                toptier_agency_tracker[key]["subtier_agencies"] = []
            if "offices" not in toptier_agency_tracker[key]:
                toptier_agency_tracker[key]["offices"] = []
            subtier_result = self._agency_office_subtier_agency_response_object(toptier_agency)
            sub_key = "".join(subtier_result)
            if sub_key not in subtiers_already_added:
                toptier_agency_tracker[key]["subtier_agencies"].append(subtier_result)
                subtiers_already_added.append(sub_key)
            if toptier_agency["office_name"] is not None and toptier_agency["office_code"] is not None:
                office_result = self._agency_office_office_response_object(toptier_agency)
                toptier_agency_tracker[key]["offices"].append(office_result)

        toptier_agency_tracker = list(toptier_agency_tracker.values())[:limit]
        toptier_agency_results = {"toptier_agency": toptier_agency_tracker}

        # Begin deriving subteir agency objects for response
        subtier_agency_filter = Q(**{self.filter_field: True}) & (
            Q(subtier_abbreviation__icontains=search_text) | Q(subtier_name__icontains=search_text)
        )

        subtier_agency_search_text_matches = (
            AgencyOfficeAutocompleteMatview.objects.filter(subtier_agency_filter).order_by(*order_by).values().all()
        )

        subtier_agency_tracker = {}
        for subtier_agency in subtier_agency_search_text_matches:
            # This key is created so that we can treat multiple records with the same
            # subtier values as a single result
            key = f'{subtier_agency["subtier_abbreviation"]}{subtier_agency["subtier_code"]}{subtier_agency["subtier_name"]}'
            if key not in subtier_agency_tracker:
                subtier_agency_tracker[key] = {}
                subtier_result = self._agency_office_subtier_agency_response_object(subtier_agency)
                subtier_agency_tracker[key] = subtier_result
            if "offices" not in subtier_agency_tracker[key]:
                subtier_agency_tracker[key]["offices"] = []
            toptier_result = self._agency_office_toptier_agency_response_object(subtier_agency)
            subtier_agency_tracker[key]["toptier_agency"] = toptier_result
            if toptier_agency["office_name"] is not None and toptier_agency["office_code"] is not None:
                office_result = self._agency_office_office_response_object(subtier_agency)
                subtier_agency_tracker[key]["offices"].append(office_result)

        subtier_agency_tracker = list(subtier_agency_tracker.values())[:limit]
        subtier_agency_results = {"subtier_agency": subtier_agency_tracker}

        # Begin deriving office objects for response
        office_filter = Q(**{self.filter_field: True}) & (Q(office_name__icontains=search_text))

        office_search_text_matches = (
            AgencyOfficeAutocompleteMatview.objects.filter(office_filter).order_by(*order_by).values().all()
        )

        office_tracker = {}
        for office in office_search_text_matches:
            # This key is created so that we can treat multiple records with the same
            # office values as a single result
            key = office["office_code"] + office["office_name"]
            if key not in office_tracker:
                office_tracker[key] = {}
                office_result = self._agency_office_office_response_object(office)
                office_tracker[key] = office_result
            subtier_result = self._agency_office_subtier_agency_response_object(office)
            office_tracker[key]["subtier_agency"] = subtier_result
            toptier_result = self._agency_office_toptier_agency_response_object(office)
            office_tracker[key]["toptier_agency"] = toptier_result

        office_tracker = list(office_tracker.values())[:limit]
        office_results = {"office": office_tracker}

        # Assemble all objects into the final response
        results = {**toptier_agency_results, **subtier_agency_results, **office_results}

        results = {"results": results}
        results["messages"] = []
        return Response(results)

    def _agency_office_subtier_agency_response_object(self, record: dict) -> dict:
        """Using the provided record, converts the subtier agency data
        into an object for the api response.

        Args:
            record: The record to extract values from.
        """
        object = {
            "abbreviation": record["subtier_abbreviation"],
            "code": record["subtier_code"],
            "name": record["subtier_name"],
        }
        return object

    def _agency_office_toptier_agency_response_object(self, record: dict) -> dict:
        """Using the provided record, converts the toptier agency data
        into an object for the api response.

        Args:
            record: The record to extract values from.
        """
        object = {
            "abbreviation": record["toptier_abbreviation"],
            "code": record["toptier_code"],
            "name": record["toptier_name"],
        }
        return object

    def _agency_office_office_response_object(self, record: dict) -> dict:
        """Using the provided record, converts the office data
        into an object for the api response.

        Args:
            record: The record to extract values from.
        """
        object = {
            "code": record["office_code"],
            "name": record["office_name"],
        }
        return object


class AwardingAgencyAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve awarding agencies matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/awarding_agency.md"
    filter_field = "has_awarding_data"

    @cache_response()
    def post(self, request):
        return self.agency_autocomplete(request)


class AwardingAgencyOfficeAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve awarding
    agencies and offices matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/awarding_agency_office.md"
    filter_field = "has_awarding_data"

    @cache_response()
    def post(self, request):
        return self.agency_office_autocomplete(request)


class FundingAgencyOfficeAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve funding
    agencies and offices matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/funding_agency_office.md"
    filter_field = "has_funding_data"

    @cache_response()
    def post(self, request):
        return self.agency_office_autocomplete(request)


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

        return Response(
            {
                "results": sorted(
                    list(queryset.values("program_number", "program_title", "popular_name")[:limit]),
                    key=lambda x: x["program_number"],
                )
            }
        )


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


class ProgramActivityAutocompleteViewSet(BaseAutocompleteViewSet):
    """
    This route sends a request to the backend to retrieve program activities and their names based
    on a search string. This may be the 4-character program activity code or a name string.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/program_activity.md"

    @cache_response()
    def post(self, request):
        search_text, limit = self.get_request_payload(request)

        queryset = RefProgramActivity.objects.distinct("program_activity_code", "program_activity_name")

        if len(search_text) == 4 and queryset.filter(program_activity_code=search_text.upper()).exists():
            queryset = queryset.filter(program_activity_code=search_text.upper())
        else:
            queryset = queryset.filter(program_activity_name__icontains=search_text)

        return Response({"results": list(queryset.values("program_activity_code", "program_activity_name")[:limit])})


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
