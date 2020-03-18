from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_location import (
    BaseLocationViewSet,
    LocationType,
)


class CountyViewSet(BaseLocationViewSet):
    """
    This route takes award filters and returns spending by County.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/county.md"

    location_type = LocationType.COUNTY
    category = Category(name="county", agg_field="pop_county_agg_field")
