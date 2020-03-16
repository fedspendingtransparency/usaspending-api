from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_industry_codes import (
    IndustryCodeType,
    BaseIndustryCodeViewSet,
)


class CfdaViewSet(BaseIndustryCodeViewSet):
    """
    This route takes award filters and returns spending by CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/cfda.md"

    industry_code_type = IndustryCodeType.CFDA
    category = Category(name="cfda", agg_field="cfda_agg_field")
