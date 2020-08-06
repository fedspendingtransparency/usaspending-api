from abc import ABCMeta
from decimal import Decimal
from django.db.models import QuerySet
from enum import Enum
from typing import List

from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    Category,
    AbstractSpendingByCategoryViewSet,
)


class AccountType(Enum):
    FEDERAL_ACCOUNT = "federal_account"


class AbstractAccountViewSet(AbstractSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Abstract class used by Federal Accounts spending_by_category endpoint
    """

    account_type: AccountType

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        account_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in account_info_buckets:
            account_info = json_str_to_dict(bucket.get("key"))
            results.append(
                {
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "id": int(account_info.get("id")) if account_info.get("id") else None,
                    "code": account_info.get("federal_account_code") or None,
                    "name": account_info.get("account_title"),
                }
            )

        return results

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        self._raise_not_implemented()


class FederalAccountViewSet(AbstractAccountViewSet):
    """
    This route takes award filters and returns spending by Federal Account.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/federal_account.md"

    category = Category(name="federal_account", agg_key="federal_accounts")
