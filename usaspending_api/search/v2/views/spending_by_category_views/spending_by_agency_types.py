from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import F
from django.utils.text import slugify

from usaspending_api.references.models import SubtierAgency, ToptierAgency
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    AbstractSpendingByCategoryViewSet,
    Category,
)


class AgencyType(Enum):
    AWARDING_TOPTIER = "awarding_toptier"
    AWARDING_SUBTIER = "awarding_subtier"
    FUNDING_TOPTIER = "funding_toptier"
    FUNDING_SUBTIER = "funding_subtier"


class AbstractAgencyViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Abstract class used by the different Awarding / Funding Agencies and Subagencies
    """

    agency_type: AgencyType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        # Get the codes
        agency_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        code_list = [bucket.get("key") for bucket in agency_info_buckets if bucket.get("key")]

        # Get the current agency info
        current_agency_info = {}
        if self.agency_type in (AgencyType.AWARDING_TOPTIER, AgencyType.FUNDING_TOPTIER):
            agency_info_query = (
                ToptierAgency.objects.filter(toptier_code__in=code_list, agency__toptier_flag=True)
                .annotate(
                    id=F("agency__id"),
                    code=F("abbreviation"),
                    name_for_slug=F("agency__toptieragencypublisheddabsview__name"),
                )
                .values("id", "code", "name", "toptier_code", "name_for_slug")
            )
            for res in agency_info_query:
                toptier_code = res.pop("toptier_code")
                name_for_slug = res.pop("name_for_slug")
                current_agency_info[toptier_code] = {
                    **res,
                    "agency_slug": None if name_for_slug is None else slugify(name_for_slug),
                }
        else:
            agency_info_query = (
                SubtierAgency.objects.filter(subtier_code__in=code_list)
                .annotate(
                    id=F("agency__id"),
                    code=F("abbreviation"),
                    agency_id=F("agency__toptier_agency_id"),
                    agency_abbreviation=F("agency__toptier_agency__abbreviation"),
                    agency_name=F("agency__toptier_agency__name"),
                )
                .values("subtier_code", "id", "code", "name", "agency_id", "agency_abbreviation", "agency_name")
            )
            for res in agency_info_query:
                subtier_code = res.pop("subtier_code")
                current_agency_info[subtier_code] = {
                    **res,
                    "agency_slug": slugify(res["agency_name"]),
                    "subagency_slug": slugify(res["name"]),
                }

        # Build out the results
        results = []
        for bucket in agency_info_buckets:
            agency_info = current_agency_info.get(bucket.get("key")) or {}
            results.append(
                {
                    **agency_info,
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    # Outlays only apply to Awards. Transactions and Subawards should be `None`.
                    "total_outlays": bucket.get("sum_as_dollars_outlay", {"value": None})["value"],
                }
            )

        return results


class AwardingAgencyViewSet(AbstractAgencyViewSet):
    """
    This route takes award filters and returns spending by awarding agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/awarding_agency.md"

    agency_type = AgencyType.AWARDING_TOPTIER
    category = Category(name="awarding_agency", agg_key="awarding_toptier_agency_agg_key")


class AwardingSubagencyViewSet(AbstractAgencyViewSet):
    """
    This route takes award filters and returns spending by awarding subagencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/awarding_subagency.md"

    agency_type = AgencyType.AWARDING_SUBTIER
    category = Category(name="awarding_subagency", agg_key="awarding_subtier_agency_agg_key")


class FundingAgencyViewSet(AbstractAgencyViewSet):
    """
    This route takes award filters and returns spending by funding agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/funding_agency.md"

    agency_type = AgencyType.FUNDING_TOPTIER
    category = Category(name="funding_agency", agg_key="funding_toptier_agency_agg_key")


class FundingSubagencyViewSet(AbstractAgencyViewSet):
    """
    This route takes award filters and returns spending by funding subagencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/funding_subagency.md"

    agency_type = AgencyType.FUNDING_SUBTIER
    category = Category(name="funding_subagency", agg_key="funding_subtier_agency_agg_key")
