from decimal import Decimal
from typing import List

from django.db.models import Case, F, IntegerField, QuerySet, Value, When
from django.utils.decorators import method_decorator

from usaspending_api.common.api_versioning import deprecated
from usaspending_api.common.recipient_lookups import combine_recipient_hash_and_level
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
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

    @staticmethod
    def _get_recipient_id(row: list) -> str:
        """
        In the recipient_profile table there is a 1 to 1 relationship between hashes and DUNS
        (recipient_unique_id) and the hashes+duns match exactly between recipient_profile and
        recipient_lookup where there are matches.  Grab the level from recipient_profile by
        hash if we have one or by DUNS if we have one of those.
        """
        if "recipient_hash" in row:
            profile_filter = {"recipient_hash": row["recipient_hash"]}
        elif "recipient_unique_id" in row:
            profile_filter = {"recipient_unique_id": row["recipient_unique_id"]}
        elif "sub_awardee_or_recipient_uniqu" in row:
            profile_filter = {"recipient_unique_id": row["sub_awardee_or_recipient_uniqu"]}
        else:
            raise RuntimeError(
                "Attempted to lookup recipient profile using a queryset that contains neither "
                "'recipient_hash' nor 'recipient_unique_id' nor 'sub_awardee_or_recipient_uniqu'"
            )

        profile = (
            RecipientProfile.objects.filter(**profile_filter)
            .exclude(recipient_name__in=SPECIAL_CASES)
            .annotate(
                sort_order=Case(
                    When(recipient_level="C", then=Value(0)),
                    When(recipient_level="R", then=Value(1)),
                    default=Value(2),
                    output_field=IntegerField(),
                )
            )
            .values("recipient_hash", "recipient_level")
            .order_by("sort_order")
            .first()
        )

        return (
            combine_recipient_hash_and_level(profile["recipient_hash"], profile["recipient_level"]) if profile else None
        )

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

    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        django_filters = {}
        django_values = ["sub_awardee_or_recipient_legal", "sub_awardee_or_recipient_uniqu"]
        annotations = {"name": F("sub_awardee_or_recipient_legal"), "code": F("sub_awardee_or_recipient_uniqu")}
        queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(**annotations)
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])

        for row in query_results:
            row["recipient_id"] = self._get_recipient_id(row)

            for key in django_values:
                del row[key]

        return query_results


@method_decorator(deprecated, name="post")
class RecipientDunsViewSet(RecipientViewSet):
    """
    <em>Deprecated: Please see <a href="../recipient">this endpoint</a> instead.</em>

    This route takes award filters and returns spending by Recipient DUNS.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/recipient_duns.md"
    category = Category(name="recipient_duns", agg_key="recipient_agg_key")
