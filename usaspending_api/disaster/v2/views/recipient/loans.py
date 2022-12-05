from ast import literal_eval
from typing import List

from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)
from usaspending_api.recipient.models import RecipientLookup
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class RecipientLoansViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Query text and returns Loans by Recipient.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/loans.md"

    required_filters = ["def_codes", "query", "_loan_award_type_codes"]
    query_fields = ["recipient_name.contains"]
    agg_key = "recipient_agg_key"

    sum_column_mapping: List[str]  # Set in the pagination mixin

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            # Build a list of hash IDs to handle multiple levels
            if "special" in bucket.get("key"):
                recipient_info = bucket.get("key").split("/")
                recipient_name = recipient_info[1]
                recipient_duns = recipient_info[2]
                recipient_hash_list = None
            else:
                recipient_info = bucket.get("key").split("/")
                recipient_hash = recipient_info[0]
                recipient_levels = literal_eval(recipient_info[1])
                recipient_hash_list = [f"{recipient_hash}-{level}" for level in recipient_levels]
                info = RecipientLookup.objects.get(recipient_hash=recipient_hash)
                recipient_name = info.legal_business_name
                recipient_duns = info.duns
            results.append(
                {
                    "id": recipient_hash_list,
                    "code": recipient_duns or "DUNS Number not provided",
                    "description": recipient_name or None,
                    "award_count": int(bucket.get("doc_count", 0)),
                    **{
                        column: get_summed_value_as_float(
                            bucket.get("nested", {}).get("filtered_aggs", {})
                            if column != "face_value_of_loan"
                            else bucket.get("nested", {}).get("filtered_aggs", {}).get("reverse_nested", {}),
                            self.sum_column_mapping[column],
                        )
                        for column in self.sum_column_mapping
                    },
                }
            )
        return results
