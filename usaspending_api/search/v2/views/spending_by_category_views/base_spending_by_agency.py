import json
from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import QuerySet, F

from usaspending_api.search.helpers.spending_by_category_helpers import fetch_agency_tier_id_by_agency
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
)


class AgencyType(Enum):
    AWARDING_TOPTIER = "awarding_toptier"
    AWARDING_SUBTIER = "awarding_subtier"
    FUNDING_TOPTIER = "funding_toptier"
    FUNDING_SUBTIER = "funding_subtier"


class BaseAgencyViewSet(BaseSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Base class used by the different Awarding / Funding Agencies and Subagencies
    """

    agency_type: AgencyType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        agency_info_buckets = response.get("group_by_agg_field", {}).get("buckets", [])
        for bucket in agency_info_buckets:
            agency_info = json.loads(bucket.get("key"))

            results.append(
                {
                    "amount": Decimal(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "name": agency_info.get("name"),
                    "code": agency_info.get("abbreviation") or None,
                    "id": int(agency_info.get("id")),
                }
            )

        return results

    def query_django(self, base_queryset: QuerySet) -> List[dict]:
        django_filters = {f"{self.agency_type.value}_agency_name__isnull": False}
        django_values = [f"{self.agency_type.value}_agency_name", f"{self.agency_type.value}_agency_abbreviation"]
        queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(
            name=F(f"{self.agency_type.value}_agency_name"), code=F(f"{self.agency_type.value}_agency_abbreviation")
        )
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])
        for row in query_results:
            is_subtier = (
                self.agency_type == AgencyType.AWARDING_SUBTIER or self.agency_type == AgencyType.FUNDING_SUBTIER
            )
            row["id"] = fetch_agency_tier_id_by_agency(agency_name=row["name"], is_subtier=is_subtier)
            row.pop(f"{self.agency_type.value}_agency_name")
            row.pop(f"{self.agency_type.value}_agency_abbreviation")
        return query_results
