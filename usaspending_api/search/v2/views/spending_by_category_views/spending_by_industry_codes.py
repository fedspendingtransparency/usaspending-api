from abc import ABCMeta
from dataclasses import replace
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import F

from usaspending_api.references.models import NAICS, PSC, Cfda, DisasterEmergencyFundCode
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
        if self.category.nested_path:
            response = response.get("nested_agg")
        if self.category.filter_key_to_limit:
            response = response.get("filter_agg")
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
                        if self.spending_level == SpendingLevel.AWARD or self.spending_level == SpendingLevel.FILE_C
                        else None
                    ),
                }
            )
        return results


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

    def post(self, request, *args, **kwargs):
        validated_payload = self.validate_payload(request)
        nested_path = "spending_by_defc"
        if self.spending_level == SpendingLevel.FILE_C:
            self.category = replace(
                self.category,
                nested_path=nested_path,
                agg_key=f"{nested_path}.defc",
                agg_key_suffix="",
                obligation_field=f"{nested_path}.obligation",
                outlay_field=f"{nested_path}.outlay",
                filter_key_to_limit="def_codes",
            )

        return super().post(request, validated_payload=validated_payload)
