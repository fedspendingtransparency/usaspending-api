from abc import ABCMeta
from decimal import Decimal
from django.db.models import QuerySet, F
from enum import Enum
from typing import List

from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_country_name_from_code,
    fetch_state_name_from_code,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    Category,
    AbstractSpendingByCategoryViewSet,
)
from usaspending_api.references.models import RefCountryCode, PopCounty, PopCongressionalDistrict
from usaspending_api.recipient.models import StateData


class LocationType(Enum):
    COUNTRY = "country"
    STATE_TERRITORY = "state"
    COUNTY = "county"
    CONGRESSIONAL_DISTRICT = "congressional"


class AbstractLocationViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Base class used by the different spending by Location endpoints
    """

    location_type: LocationType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        # Get the codes
        location_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        code_list = [bucket.get("key") for bucket in location_info_buckets if bucket.get("key")]

        # Get the current location info
        current_location_info = {}
        if self.location_type == LocationType.COUNTRY:
            location_info_query = (
                RefCountryCode.objects.filter(country_code__in=code_list)
                .annotate(name=F("country_name"), code=F("country_code"))
                .values("name", "code")
            )
        elif self.location_type == LocationType.STATE_TERRITORY:
            location_info_query = StateData.objects.filter(code__in=code_list).values("name", "code")
        elif self.location_type == LocationType.COUNTY:
            location_info_query = (
                PopCounty.objects.filter(county_number__in=code_list)
                .annotate(name=F("county_name"), code=F("county_number"))
                .values("name", "code")
            )
        elif self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
            location_info_query = PopCongressionalDistrict.objects.filter(congressional_district__in=code_list).values(
                "state_code", "congressional_district"
            )
            # can't do the distict transformation in the queryset, see below
        for location_info in location_info_query.all():
            if self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
                location_info["code"] = location_info.get("congressional_district") or ""
                display_code = "MULTIPLE DISTRICTS" if location_info["code"] == "90" else location_info["code"]
                location_info["name"] = f"{location_info.get('state_code') or ''}-{display_code}"
            if self.location_type == LocationType.STATE_TERRITORY:
                location_info["name"] = location_info["name"].title()
            else:
                location_info["name"] = location_info["name"].upper()
            current_location_info[location_info["code"]] = location_info

        # Build out the results
        results = []
        for bucket in location_info_buckets:
            location_info = current_location_info.get(bucket.get("key")) or {}
            results.append(
                {
                    "id": None,
                    "code": location_info.get("code"),
                    "name": location_info.get("name"),
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                }
            )
        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        subaward_mappings = {
            LocationType.COUNTY: "sub_place_of_perform_county_code",
            LocationType.CONGRESSIONAL_DISTRICT: "sub_place_of_perform_congressio",
            LocationType.STATE_TERRITORY: "sub_place_of_perform_state_code",
            LocationType.COUNTRY: "sub_place_of_perform_country_co",
        }
        django_filters = {f"{subaward_mappings[self.location_type]}__isnull": False}

        if self.location_type == LocationType.COUNTY:
            django_values = [
                "sub_place_of_perform_country_co",
                "sub_place_of_perform_state_code",
                "sub_place_of_perform_county_code",
                "sub_place_of_perform_county_name",
            ]
            annotations = {"code": F("sub_place_of_perform_county_code"), "name": F("sub_place_of_perform_county_name")}
        elif self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
            django_values = [
                "sub_place_of_perform_country_co",
                "sub_place_of_perform_state_code",
                "sub_place_of_perform_congressio",
            ]
            annotations = {"code": F("sub_place_of_perform_congressio")}
        elif self.location_type == LocationType.STATE_TERRITORY:
            django_values = ["sub_place_of_perform_country_co", "sub_place_of_perform_state_code"]
            annotations = {"code": F("sub_place_of_perform_state_code")}
        else:
            django_values = [f"sub_place_of_perform_country_co"]
            annotations = {"code": F(f"sub_place_of_perform_country_co")}

        queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(**annotations)
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])

        for row in query_results:
            row["id"] = None
            if self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
                district_code = row["code"]
                if district_code == "90":
                    district_code = "MULTIPLE DISTRICTS"
                row["name"] = f"{row['sub_place_of_perform_state_code']}-{district_code}"
            elif self.location_type == LocationType.COUNTRY:
                row["name"] = fetch_country_name_from_code(row["code"])
            elif self.location_type == LocationType.STATE_TERRITORY:
                row["name"] = fetch_state_name_from_code(row["code"])

            for key in django_values:
                row.pop(key)

        return query_results


class CountyViewSet(AbstractLocationViewSet):
    """
    This route takes award filters and returns spending by County.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/county.md"

    location_type = LocationType.COUNTY
    category = Category(name="county", agg_key="pop_county_agg_key")


class DistrictViewSet(AbstractLocationViewSet):
    """
    This route takes award filters and returns spending by Congressional District.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/district.md"

    location_type = LocationType.CONGRESSIONAL_DISTRICT
    category = Category(name="district", agg_key="pop_congressional_agg_key")


class StateTerritoryViewSet(AbstractLocationViewSet):
    """
    This route takes award filters and returns spending by State Territory.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/state_territory.md"

    location_type = LocationType.STATE_TERRITORY
    category = Category(name="state_territory", agg_key="pop_state_agg_key")


class CountryViewSet(AbstractLocationViewSet):
    """
    This route takes award filters and returns spending by Country.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/country.md"

    location_type = LocationType.COUNTRY
    category = Category(name="country", agg_key="pop_country_agg_key")
