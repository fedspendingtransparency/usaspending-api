import json

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
        results = []
        location_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in location_info_buckets:
            location_info = json.loads(bucket.get("key"))

            if self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
                if location_info.get("congressional_code") == "90":
                    congressional_code = "MULTIPLE DISTRICTS"
                else:
                    congressional_code = location_info.get("congressional_code") or ""
                name = f"{location_info.get('state_code') or ''}-{congressional_code}"
            else:
                name = location_info.get(f"{self.location_type.value}_name") or ""

            if self.location_type == LocationType.STATE_TERRITORY:
                name = name.title()
            else:
                name = name.upper()
            results.append(
                {
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "code": location_info.get(f"{self.location_type.value}_code"),
                    "id": None,
                    "name": name,
                }
            )

        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        django_filters = {f"pop_{self.location_type.value}_code__isnull": False}

        if self.location_type == LocationType.COUNTY:
            django_values = ["pop_country_code", "pop_state_code", "pop_county_code", "pop_county_name"]
            annotations = {"code": F("pop_county_code"), "name": F("pop_county_name")}
        elif self.location_type == LocationType.CONGRESSIONAL_DISTRICT:
            django_values = ["pop_country_code", "pop_state_code", "pop_congressional_code"]
            annotations = {"code": F("pop_congressional_code")}
        elif self.location_type == LocationType.STATE_TERRITORY:
            django_values = ["pop_country_code", "pop_state_code"]
            annotations = {"code": F("pop_state_code")}
        else:
            django_values = [f"pop_country_code"]
            annotations = {"code": F(f"pop_country_code")}

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
                row["name"] = f"{row['pop_state_code']}-{district_code}"
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
