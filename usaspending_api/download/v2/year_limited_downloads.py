from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings, all_subaward_types
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet
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

        return BaseDownloadViewSet.post(self, request, "award", "bulk_download")

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
        if filters.get("prime_award_types") is None and filters.get("sub_award_types") is None:
            raise InvalidParameterException(
                "Missing one or more required body parameters: prime_award_types or sub_award_types"
            )

        # Validate other parameters previously required by the Bulk Download endpoint
        for required_param in ["agency", "date_type", "date_range"]:
            if required_param not in filters:
                raise InvalidParameterException(f"Missing one or more required body parameters: {required_param}")

        # Replacing award_types with award_type_codes and creating new filter for
        # custom award download to keep Prime and Sub Awards separate;
        # Also adding award levels based on filters passed
        request_data["award_levels"] = []
        filters["prime_and_sub_award_types"] = {}
        if filters.get("prime_award_types"):
            prime_award_type_codes = []
            try:
                for prime_award_type_code in filters["prime_award_types"]:
                    if prime_award_type_code in all_award_types_mappings:
                        prime_award_type_codes.extend(all_award_types_mappings[prime_award_type_code])
                    else:
                        raise InvalidParameterException(f"Invalid prime_award_type: {prime_award_type_code}")
                del filters["prime_award_types"]
                filters["prime_and_sub_award_types"]["prime_awards"] = prime_award_type_codes
                request_data["award_levels"].append("prime_awards")
            except TypeError:
                raise InvalidParameterException("prime_award_types parameter not provided as a list")

        if filters.get("sub_award_types"):
            sub_award_types = []
            try:
                for sub_award_type in filters["sub_award_types"]:
                    if sub_award_type in all_subaward_types:
                        sub_award_types.append(sub_award_type)
                    else:
                        raise InvalidParameterException(f"Invalid sub_award_type: {sub_award_type}")
                del filters["sub_award_types"]
                filters["prime_and_sub_award_types"]["sub_awards"] = sub_award_types
                request_data["award_levels"].append("sub_awards")
            except TypeError:
                raise InvalidParameterException("sub_award_types parameter not provided as a list")

        # Replacing date_range with time_period
        date_range_copied = filters["date_range"].copy()
        date_range_copied["date_type"] = filters["date_type"]
        filters["time_period"] = [date_range_copied]
        del filters["date_range"]
        del filters["date_type"]

        # Replacing agency with agencies
        if filters["agency"] != "all":
            toptier_name = ToptierAgency.objects.filter(toptier_agency_id=filters["agency"]).values("name")
            if not toptier_name:
                raise InvalidParameterException("Toptier ID not found: {}".format(filters["agency"]))
            toptier_name = toptier_name[0]["name"]
            if "sub_agency" in filters:
                if filters["sub_agency"]:
                    filters["agencies"] = [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": filters["sub_agency"],
                            "toptier_name": toptier_name,
                        }
                    ]
                del filters["sub_agency"]
            else:
                filters["agencies"] = [{"type": "awarding", "tier": "toptier", "name": toptier_name}]

        request_data["filters"] = filters
