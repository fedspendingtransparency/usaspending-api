from typing import List

from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)
from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class CfdaLoansViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Query text and returns Loans of CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/cfda/loans.md"

    required_filters = ["def_codes", "query", "_loan_award_type_codes"]
    query_fields = ["cfda_title", "cfda_number"]
    agg_key = "cfda_agg_key"
    is_loans = True

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in info_buckets:
            info = json_str_to_dict(bucket.get("key"))
            results.append(
                {
                    "id": int(info.get("id")) if info.get("id") else None,
                    "code": info.get("code") or None,
                    "description": info.get("description") or None,
                    "award_count": int(bucket.get("doc_count", 0)),
                    "resource_link": info.get("url") or None,
                    **{
                        column: get_summed_value_as_float(bucket, self.sum_column_mapping[column])
                        for column in self.sum_column_mapping
                    },
                }
            )

        return results
