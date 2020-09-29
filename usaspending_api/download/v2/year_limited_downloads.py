from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet, DownloadRequestType
from usaspending_api.references.models import ToptierAgency


class YearLimitedDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md"

    def post(self, request):
        request.data["constraint_type"] = "year"

        # TODO: update front end to use the Common Filter Object and get rid of this function
        self.process_filters(request.data)

        return BaseDownloadViewSet.post(self, request, DownloadRequestType.AWARD, "bulk_download")

    def process_filters(self, request_data):
        """Filter function to update Bulk Download parameters to shared parameters"""

        # Validate filter parameter
        filters = request_data.get("filters", None)
        if not filters:
            raise InvalidParameterException("Missing one or more required body parameters: filters")

        # Validate keyword search first, remove all other filters
        keyword_filter = filters.get("keyword", None) or filters.get("keywords", None)
        if keyword_filter and len(filters.keys()) == 1:
            request_data["filters"] = {"elasticsearch_keyword": keyword_filter}
            return

        # Validate award and subaward type separately since only one is required
        prime_award_types = filters.get("prime_award_types")
        sub_award_types = filters.get("sub_award_types")

        if prime_award_types is None and sub_award_types is None:
            raise InvalidParameterException(
                "Missing one or more required body parameters: prime_award_types or sub_award_types"
            )

        # Validate other parameters previously required by the Bulk Download endpoint
        for required_param in ["date_type", "date_range"]:
            if required_param not in filters:
                raise InvalidParameterException(f"Missing one or more required body parameters: {required_param}")

        # Replacing agency with agencies
        if "agency" in filters:
            if "agencies" not in filters:
                filters["agencies"] = []
            if str(filters["agency"]).lower() == "all":
                toptier_name = "all"
            else:
                toptier_name = ToptierAgency.objects.filter(toptier_agency_id=filters["agency"]).values("name").first()
                if toptier_name is None:
                    raise InvalidParameterException(f"Toptier ID not found: {filters['agency']}")
                toptier_name = toptier_name["name"]
            if "sub_agency" in filters:
                filters["agencies"].append(
                    {
                        "type": "awarding",
                        "tier": "subtier",
                        "name": filters["sub_agency"],
                        "toptier_name": toptier_name,
                    }
                )
                del filters["sub_agency"]
            else:
                filters["agencies"].append({"type": "awarding", "tier": "toptier", "name": toptier_name})
            del filters["agency"]

        if "agencies" in filters:
            filters["agencies"] = [val for val in filters["agencies"] if val.get("name", "").lower() != "all"]
        else:
            raise InvalidParameterException("Request must include either 'agency' or 'agencies'")

        # Creating new filter for custom award download to keep Prime and Sub Awards separate;
        # Also adding award levels based on filters passed
        request_data["award_levels"] = []
        filters["prime_and_sub_award_types"] = {}

        if prime_award_types is not None:
            try:
                if len(prime_award_types) > 0:
                    filters["prime_and_sub_award_types"]["prime_awards"] = prime_award_types
                    request_data["award_levels"].append("prime_awards")
                del filters["prime_award_types"]
            except TypeError:
                raise InvalidParameterException("prime_award_types parameter not provided as a list")

        if sub_award_types is not None:
            try:
                if len(sub_award_types) > 0:
                    filters["prime_and_sub_award_types"]["sub_awards"] = sub_award_types
                    request_data["award_levels"].append("sub_awards")
                del filters["sub_award_types"]
            except TypeError:
                raise InvalidParameterException("sub_award_types parameter not provided as a list")

        # Replacing date_range with time_period
        date_range_copied = filters["date_range"].copy()
        date_range_copied["date_type"] = filters["date_type"]
        filters["time_period"] = [date_range_copied]
        del filters["date_range"]
        del filters["date_type"]

        request_data["filters"] = filters
