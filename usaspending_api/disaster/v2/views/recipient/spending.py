from ast import literal_eval
from typing import List

from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.recipient.models import RecipientLookup
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class RecipientSpendingViewSet(ElasticsearchSpendingPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes, Query text, and Award Type Codes and returns Spending by Recipient.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/spending.md"

    required_filters = ["def_codes", "award_type_codes", "query"]
    query_fields = ["recipient_name.contains"]
    agg_key = "recipient_agg_key"

    sum_column_mapping: List[str]  # Set in the pagination mixin

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            # Build a list of hash IDs to handle multiple levels
            recipient_info = bucket.get("key").split("/")
            recipient_hash = recipient_info[0]
            recipient_levels = literal_eval(recipient_info[1]) if len(recipient_info) > 1 else None
            recipient_hash_list = (
                [f"{recipient_hash}-{level}" for level in recipient_levels] if recipient_levels else None
            )
            info = RecipientLookup.objects.filter(recipient_hash=recipient_hash).first()
            recipient_name = info.legal_business_name if info else None
            recipient_duns = info.duns if info else None
            if recipient_name in [
                "MULTIPLE RECIPIENTS",
                "REDACTED DUE TO PII",
                "MULTIPLE FOREIGN RECIPIENTS",
                "PRIVATE INDIVIDUAL",
                "INDIVIDUAL RECIPIENT",
                "MISCELLANEOUS FOREIGN AWARDEES",
            ]:
                recipient_hash_list = None

            results.append(
                {
                    "id": recipient_hash_list,
                    "code": recipient_duns or "DUNS Number not provided",
                    "description": recipient_name,
                    "award_count": int(bucket.get("doc_count", 0)),
                    **{
                        column: get_summed_value_as_float(
                            (
                                bucket.get("nested", {}).get("filtered_aggs", {})
                                if column != "face_value_of_loan"
                                else bucket.get("nested", {}).get("filtered_aggs", {}).get("reverse_nested", {})
                            ),
                            self.sum_column_mapping[column],
                        )
                        for column in self.sum_column_mapping
                    },
                }
            )

        return results
