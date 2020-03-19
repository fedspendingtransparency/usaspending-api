import json
from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import QuerySet, F

from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_naics_description_from_code,
    fetch_psc_description_by_code,
    fetch_cfda_id_title_by_number,
)
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    AbstractSpendingByCategoryViewSet,
)


class IndustryCodeType(Enum):
    CFDA = "cfda_number"
    PSC = "product_or_service_code"
    NAICS = "naics_code"


class AbstractIndustryCodeViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Base class used by the different spending by Industry Code endpoints
    """

    industry_code_type: IndustryCodeType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        industry_code_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in industry_code_info_buckets:
            industry_code_info = json.loads(bucket.get("key"))

            results.append(
                {
                    "amount": Decimal(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "code": industry_code_info.get("code"),
                    "id": int(industry_code_info.get("id")) if len(industry_code_info.get("id")) > 0 else None,
                    "name": industry_code_info.get("description") or None,
                }
            )

        return results

    def query_django(self, base_queryset: QuerySet) -> List[dict]:
        if self.subawards:
            if self.industry_code_type == IndustryCodeType.PSC or self.industry_code_type == IndustryCodeType.NAICS:
                self._raise_not_implemented()

        django_filters = {f"{self.industry_code_type.value}__isnull": False}
        django_values = [self.industry_code_type.value]

        queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(
            code=F(self.industry_code_type.value)
        )
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])

        for row in query_results:
            if self.industry_code_type == IndustryCodeType.CFDA:
                row["id"], row["name"] = fetch_cfda_id_title_by_number(row["code"])
            elif self.industry_code_type == IndustryCodeType.PSC:
                row["id"] = None
                row["name"] = fetch_psc_description_by_code(row["code"])
            elif self.industry_code_type == IndustryCodeType.NAICS:
                row["id"] = None
                row["name"] = fetch_naics_description_from_code(row["code"], row["name"])
            row.pop(self.industry_code_type.value)

        return query_results
