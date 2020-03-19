import json
from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import QuerySet, F

from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_state_name_from_code,
    fetch_country_name_from_code,
)
from usaspending_api.search.v2.views.spending_by_category_views.abstract_spending_by_category import (
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
                name = f"{location_info.get('state_code')}-{location_info.get('congressional_code')}"
            else:
                name = location_info.get(f"{self.location_type.value}_name")

            if self.location_type == LocationType.STATE_TERRITORY:
                name = name.title()
            else:
                name = name.upper()

            results.append(
                {
                    "amount": Decimal(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "code": location_info.get(f"{self.location_type.value}_code"),
                    "name": name,
                    "id": None,
                }
            )

        return results

    def query_django(self, base_queryset: QuerySet) -> List[dict]:
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
