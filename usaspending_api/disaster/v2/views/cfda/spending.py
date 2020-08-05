import json
from decimal import Decimal
from typing import List

from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)


class CfdaSpendingViewSet(ElasticsearchSpendingPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes, Award Type Codes, and Query text and returns Spending of CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/cfda/spending.md"

    required_filters = ["def_codes", "_assistance_award_type_codes", "query"]
    query_fields = ["cfda_title", "cfda_number"]
    agg_key = "cfda_agg_key"

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in info_buckets:
            info = json.loads(bucket.get("key").encode("unicode_escape"))
            results.append(
                {
                    "id": int(info.get("id")) if info.get("id") else None,
                    "code": info.get("code") or None,
                    "description": info.get("description") or None,
                    "award_count": int(bucket.get("doc_count", 0)),
                    "resource_link": info.get("url") or None,
                    **{
                        column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                        for column in self.sum_column_mapping
                    },
                }
            )

        return results
