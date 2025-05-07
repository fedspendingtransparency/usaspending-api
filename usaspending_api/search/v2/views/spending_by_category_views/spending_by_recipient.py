from decimal import Decimal
from typing import List

from django.utils.decorators import method_decorator

from usaspending_api.common.api_versioning import deprecated
from usaspending_api.recipient.models import RecipientLookup
from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_category import (
    AbstractSpendingByCategoryViewSet,
    Category,
)


class RecipientViewSet(AbstractSpendingByCategoryViewSet):
    """
    This route takes award filters and returns spending by Recipient UEI.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/recipient.md"
    category = Category(name="recipient", agg_key="recipient_agg_key")

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        # Get the codes
        recipient_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        recipient_hashes = [
            bucket.get("key").split("/")[0] for bucket in recipient_info_buckets if bucket.get("key") != ""
        ]

        # Get the current recipient info
        current_recipient_info = {}
        recipient_info_query = RecipientLookup.objects.filter(recipient_hash__in=recipient_hashes).values(
            "duns", "legal_business_name", "recipient_hash", "uei"
        )
        for recipient_info in recipient_info_query.all():
            current_recipient_info[str(recipient_info["recipient_hash"])] = recipient_info

        # Build out the results
        results = []
        for bucket in recipient_info_buckets:
            result_hash, result_level = tuple(bucket.get("key").split("/")) if bucket.get("key") else (None, None)
            result_hash_with_level = f"{result_hash}-{result_level}" if (result_hash and result_level) else None
            recipient_info = current_recipient_info.get(result_hash) or {}

            results.append(
                {
                    "amount": int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100"),
                    "recipient_id": result_hash_with_level,
                    "name": recipient_info.get("legal_business_name", None),
                    "code": recipient_info.get("duns", None),
                    "uei": recipient_info.get("uei", None),
                    "total_outlays": (
                        bucket.get("sum_as_dollars_outlay", {"value": None}).get("value")
                        if self.spending_level == SpendingLevel.AWARD
                        else None
                    ),
                }
            )

        return results


@method_decorator(deprecated, name="post")
class RecipientDunsViewSet(RecipientViewSet):
    """
    <em>Deprecated: Please see <a href="../recipient">this endpoint</a> instead.</em>

    This route takes award filters and returns spending by Recipient DUNS.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/recipient_duns.md"
    category = Category(name="recipient_duns", agg_key="recipient_agg_key")
