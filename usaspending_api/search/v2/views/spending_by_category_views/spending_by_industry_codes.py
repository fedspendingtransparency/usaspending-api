from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import F, QuerySet

from usaspending_api.references.models import NAICS, PSC, Cfda, DisasterEmergencyFundCode
from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_cfda_id_title_by_number,
    fetch_defc_title_by_code,
    fetch_naics_description_from_code,
    fetch_psc_description_by_code,
)
from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    AbstractSpendingByCategoryViewSet,
    Category,
)


class IndustryCodeType(Enum):
    CFDA = "cfda_number"
    PSC = "product_or_service_code"
    NAICS = "naics_code"
    DEFC = "disaster_emergency_fund_codes"


class AbstractIndustryCodeViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Abstract class used by the different spending by Industry Code endpoints
    """

    industry_code_type: IndustryCodeType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        # Get the codes
        industry_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        code_list = [bucket.get("key") for bucket in industry_info_buckets if bucket.get("key")]

        # Get the current industry info
        current_industry_info = {}
        if self.industry_code_type == IndustryCodeType.CFDA:
            industry_info_query = (
                Cfda.objects.filter(program_number__in=code_list)
                .annotate(code=F("program_number"), name=F("program_title"))
                .values("id", "code", "name")
            )
        elif self.industry_code_type == IndustryCodeType.PSC:
            industry_info_query = (
                PSC.objects.filter(code__in=code_list)
                .annotate(
                    # doesn't have ID, set to None
                    name=F("description"),
                )
                .values("code", "name")
            )
        elif self.industry_code_type == IndustryCodeType.NAICS:
            industry_info_query = (
                NAICS.objects.filter(code__in=code_list)
                .annotate(
                    # doesn't have ID, set to None
                    name=F("description"),
                )
                .values("code", "name")
            )
        else:
            industry_info_query = (
                DisasterEmergencyFundCode.objects.filter(code__in=code_list)
                .annotate(name=F("title"))
                .values("code", "name")
            )
        for industry_info in industry_info_query.all():
            current_industry_info[industry_info["code"]] = industry_info

        # Build out the results
        results = []
        for bucket in industry_info_buckets:
            industry_info = current_industry_info.get(bucket.get("key")) or {}
            results.append(
                {
                    "id": industry_info.get("id"),
                    "code": industry_info.get("code"),
                    "name": industry_info.get("name"),
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "total_outlays": (
                        bucket.get("sum_as_dollars_outlay", {"value": None}).get("value")
                        if self.spending_level == SpendingLevel.AWARD
                        else None
                    ),
                }
            )
        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        if self.industry_code_type == IndustryCodeType.PSC or self.industry_code_type == IndustryCodeType.NAICS:
            self._raise_not_implemented()

        if self.industry_code_type == IndustryCodeType.DEFC:
            return custom_defc_query(self, base_queryset)
        else:
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


def custom_defc_query(self, base_queryset):
    django_filters = {"award__disaster_emergency_fund_codes__isnull": False}
    django_values = ["award__disaster_emergency_fund_codes"]
    queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(
        code=F("award__disaster_emergency_fund_codes")
    )

    lower_limit = self.pagination.lower_limit
    upper_limit = self.pagination.upper_limit
    query_results = list(queryset[lower_limit:upper_limit])

    output_map = {}
    # Iterate over each of the codes within the results
    for grouping in query_results:
        for defc in grouping["code"]:
            # If we've already defined the code in the output map, increment it.
            # Otherwise initialize it as the result amount
            if defc in output_map:
                output_map[defc] += grouping["amount"]
            else:
                output_map[defc] = grouping["amount"]
    transformed_list = [
        {
            "id": None,
            "name": fetch_defc_title_by_code(key),
            "code": key,
            "amount": value,
        }
        for idx, (key, value) in enumerate(output_map.items())
    ]
    return transformed_list


class CfdaViewSet(AbstractIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/cfda.md"

    industry_code_type = IndustryCodeType.CFDA
    category = Category(name="cfda", agg_key="cfda_agg_key")


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


class DEFCViewSet(AbstractIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by DEFC.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/defc.md"

    industry_code_type = IndustryCodeType.DEFC
    category = Category(name="defc", agg_key="defc_agg_key")
