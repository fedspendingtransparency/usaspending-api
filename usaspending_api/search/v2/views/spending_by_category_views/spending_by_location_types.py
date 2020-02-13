from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_location import (
    LocationType,
    BaseLocationViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category


class CountryViewSet(BaseLocationViewSet):
    """
    This route takes award filters, and returns spending by awarding agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/country.md"

    location_type = LocationType.COUNTRY

    category = Category(name="country", primary_field="pop_country_code")
