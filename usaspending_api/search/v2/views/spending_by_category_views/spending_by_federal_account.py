import json
from abc import ABCMeta
from decimal import Decimal
from enum import Enum
from typing import List

from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    AbstractSpendingByCategoryViewSet,
    Category,
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
            account_info = json.loads(bucket.get("key"))
            results.append(
                {
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "id": account_info.get("id"),
                    "code": account_info.get("federal_account_code"),
                    "name": account_info.get("account_title"),
                    "total_outlays": (
                        bucket.get("sum_as_dollars_outlay", {"value": None}).get("value")
                        if self.spending_level == SpendingLevel.AWARD
                        else None
                    ),
                }
            )

        return results


class FederalAccountViewSet(AbstractAccountViewSet):
    """
    This route takes award filters and returns spending by Federal Account.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/federal_account.md"

    category = Category(name="federal_account", agg_key="federal_accounts")
