from abc import ABCMeta
from decimal import Decimal
from django.db.models import QuerySet, F
from enum import Enum
from typing import List

from django.utils.text import slugify

from usaspending_api.references.models import ToptierAgencyPublishedDABSView, ToptierAgency, SubtierAgency
from usaspending_api.references.models.agency import Agency
from usaspending_api.search.helpers.spending_by_category_helpers import fetch_agency_tier_id_by_agency
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    Category,
    AbstractSpendingByCategoryViewSet,
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
            agency_info_query = ToptierAgency.objects.filter(
                toptier_code__in=code_list, agency__toptier_flag=True
            ).annotate(id=F("agency__id"), agency_code=F("toptier_code"), code=F("abbreviation"))

            agency_info_query = agency_info_query.values("agency_code", "id", "code", "name")
            for agency_info in agency_info_query.all():
                agency_code = agency_info.pop("agency_code")
                current_agency_info[agency_code] = agency_info

        else:
            agency_info_query = SubtierAgency.objects.filter(subtier_code__in=code_list).annotate(
                id=F("agency__id"), agency_code=F("subtier_code"), code=F("abbreviation")
            )
            # this is the list of all the subtier agencies that is returned in the elastic search and adds some fields in id, agency code and code
            # code -> SA[#]
            # agency_code -> 300[#]
            # id -> 100[#]

            agency_info_query = agency_info_query.values("agency_code", "id", "code", "name", "subtier_agency_id")
            # this says that we are only looking at these three columns of the table

            for agency_info in agency_info_query.all():
                # now we are looping through all of the different rows in the table

                agency_code = agency_info.pop("agency_code")
                # now we are getting the subtier_code which is 300[#]
                current_agency_info[agency_code] = agency_info
                # and by looking this number up in a dict we can get all of that rows associated information

                subtier_id = agency_info.get("subtier_agency_id")

                # now we need to find the toptier agency associated with the subtier agency
                # we need agency name: Awarding Toptier Agency [#]
                # we need agency code: 00[#]
                # we need agency id: 200[#]

                toptier_agency_info_query = Agency.objects.filter(subtier_agency=subtier_id)
                # using the subtier primary key, filter the agency to find the respective agency object

                toptier_agency_info_query = toptier_agency_info_query.values("toptier_agency")
                # the only value we need from the agency object is the toptier agency value

                for toptier_info in toptier_agency_info_query.all():
                    top_id = toptier_info.pop("toptier_agency")
                    # we need to get this value (in case there are multiple toptier agencies)

                    toptier_query = ToptierAgency.objects.filter(topier_agency_id=top_id).annotate(
                        top_id=F("toptier_agency_id"), top_code=F("toptier_ code"), top_name=F("name")
                    )

                    toptier_query = toptier_query.values("top_id", "top_code", "top_name")

                    current_agency_info[agency_code].append(toptier_query)

        # Build out the results
        results = []
        for bucket in agency_info_buckets:
            agency_info = current_agency_info.get(bucket.get("key")) or {}
            result = {
                "id": agency_info.get("id"),
                "code": agency_info.get("code"),
                "name": agency_info.get("name"),
                "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                # TODO: need to add the new fields here to follow the AC bc desmond said so
            }
            # Only returns a non-null value if the agency has a profile page -
            # meaning it is an agency that has at least one submission.
            if self.agency_type == AgencyType.AWARDING_TOPTIER:
                submission = ToptierAgencyPublishedDABSView.objects.filter(agency_id=agency_info.get("id")).first()
                result["agency_slug"] = slugify(agency_info.get("name")) if submission is not None else None
            results.append(result)

            if self.agency_type == AgencyType.AWARDING_SUBTIER or self.agency_type == AgencyType.FUNDING_SUBTIER:
                result["agency_name"] = (agency_info.get("top_name"),)
                result["agency_id"] = (agency_info.get("top_id"),)
                result["agency_code"] = (agency_info.get("top_code"),)
            results.append(result)
        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
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
