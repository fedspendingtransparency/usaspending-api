from abc import ABCMeta
from decimal import Decimal
from django.db.models import QuerySet, F
from enum import Enum
from typing import List

from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.references.models import Cfda
from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_cfda_id_title_by_number,
    fetch_psc_description_by_code,
    fetch_naics_description_from_code,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    Category,
    AbstractSpendingByCategoryViewSet,
)


class IndustryCodeType(Enum):
    CFDA = "cfda_number"
    PSC = "product_or_service_code"
    NAICS = "naics_code"


class AbstractIndustryCodeViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Abstract class used by the different spending by Industry Code endpoints
    """

    industry_code_type: IndustryCodeType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        cfda_code_list = []  # Separate lookup is done for CFDA to join to references_cfda during ES indexing
        industry_code_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])

        for bucket in industry_code_info_buckets:
            if self.industry_code_type == IndustryCodeType.CFDA:
                industry_code_info = {"code": bucket.get("key")}
                cfda_code_list.append(industry_code_info["code"])
            else:
                industry_code_info = json_str_to_dict(bucket.get("key"))

            results.append(
                {
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "code": industry_code_info.get("code"),
                    "id": int(industry_code_info.get("id")) if industry_code_info.get("id") else None,
                    "name": industry_code_info.get("description") or None,
                }
            )

        if cfda_code_list:
            cfda_matches = {
                cfda["program_number"]: cfda
                for cfda in Cfda.objects.filter(program_number__in=cfda_code_list).values(
                    "id", "program_number", "program_title"
                )
            }
            for val in results:
                val["id"] = cfda_matches.get(val["code"], {}).get("id")
                val["name"] = cfda_matches.get(val["code"], {}).get("program_title")

        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
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
                row["name"] = fetch_naics_description_from_code(row["code"], row.get("name"))
            row.pop(self.industry_code_type.value)

        return query_results


class CfdaViewSet(AbstractIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/cfda.md"

    industry_code_type = IndustryCodeType.CFDA
    category = Category(name="cfda", agg_key=industry_code_type.value)


class NAICSViewSet(AbstractIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by NAICS.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/naics.md"

    industry_code_type = IndustryCodeType.NAICS
    category = Category(name="naics", agg_key="naics_agg_key")


class PSCViewSet(AbstractIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by PSC.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/psc.md"

    industry_code_type = IndustryCodeType.PSC
    category = Category(name="psc", agg_key="psc_agg_key")
