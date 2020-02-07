from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_agency import (
    AgencyType,
    BaseAgencyViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category


class AwardingAgencyViewSet(BaseAgencyViewSet):
    """
    This route takes award filters, and returns spending by awarding agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/awarding_agency.md"

    agency_type = AgencyType.AWARDING_TOPTIER
    category = Category(name="awarding_agency", primary_field="awarding_toptier_agency_name.keyword")


class AwardingSubagencyViewSet(BaseAgencyViewSet):
    """
    This route takes award filters, and returns spending by awarding subagencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/awarding_subagency.md"

    agency_type = AgencyType.AWARDING_SUBTIER
    category = Category(name="awarding_subagency", primary_field="awarding_subtier_agency_name.keyword")


class FundingAgencyViewSet(BaseAgencyViewSet):
    """
    This route takes award filters, and returns spending by funding agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/funding_agency.md"

    agency_type = AgencyType.FUNDING_TOPTIER
    category = Category(name="funding_agency", primary_field="funding_toptier_agency_name.keyword")


class FundingSubagencyViewSet(BaseAgencyViewSet):
    """
    This route takes award filters, and returns spending by funding subagencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/funding_subagency.md"

    agency_type = AgencyType.FUNDING_SUBTIER
    category = Category(name="funding_subagency", primary_field="funding_subtier_agency_name.keyword")
