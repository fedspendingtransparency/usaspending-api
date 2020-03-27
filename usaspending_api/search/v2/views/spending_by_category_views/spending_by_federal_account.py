import json
from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from django.db.models import QuerySet, F

from usaspending_api.search.helpers.spending_by_category_helpers import fetch_agency_tier_id_by_agency
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    Category,
    AbstractSpendingByCategoryViewSet,
)

class AccountType(Enum):
    FEDERAL_ACCOUNT = "federal_account"

class AbstractAccountViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Abstract class used by the different Awarding / Funding Agencies and Subagencies
    """

    account_type: AccountType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        account_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in account_info_buckets:
            account_info = json.loads(bucket.get("key"))
            for info in account_info:
                results.append(
                    {
                        "amount": Decimal(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                        "id": int(info.get("id")) if info.get("id") else None,
                        "code": info.get("federal_account_code") or None,
                        "name": info.get("account_title"),
                    }
            )

        return results

    def query_django(self, base_queryset: QuerySet) -> List[dict]:
        django_filters = {f"federal_account_id__isnull": False}
        django_values = [f"federal_account_id", f"federal_account_display", f"account_title"]
        queryset = self.common_db_query(base_queryset, django_filters, django_values)
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])
        for row in query_results:
            row["id"] = row.pop("federal_account_id")
            row["code"] = row.pop("federal_account_display")
            row["name"] = row.pop("account_title")
        return query_results


class FederalAccountViewSet(AbstractAccountViewSet):
    """
    This route takes award filters and returns spending by CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/cfda.md"

    industry_code_type = AccountType.FEDERAL_ACCOUNT
    category = Category(name="federal_account", agg_key="federal_accounts_agg_key")