from decimal import Decimal
from typing import List

from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.references.models import Cfda


class CfdaSpendingViewSet(ElasticsearchSpendingPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes, Award Type Codes, and Query text and returns Spending of CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/cfda/spending.md"

    required_filters = ["def_codes", "_assistance_award_type_codes", "query"]
    query_fields = ["cfda_title.contains", "cfda_number.contains"]
    agg_key = "cfda_agg_key"

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])

        cfda_prefetch_pks = [json_str_to_dict(bucket.get("key")).get("code") for bucket in info_buckets]
        prefetched_cfdas = Cfda.objects.filter(program_number__in=cfda_prefetch_pks)

        for bucket in info_buckets:
            info = json_str_to_dict(bucket.get("key"))

            toAdd = {
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

            cfda = [cfda for cfda in prefetched_cfdas if cfda.program_number == info.get("code")]
            if cfda:
                toAdd.update(
                    {
                        "cfda_federal_agency": cfda[0].federal_agency,
                        "cfda_objectives": cfda[0].objectives,
                        "cfda_website": cfda[0].url,
                        "applicant_eligibility": cfda[0].applicant_eligibility,
                        "beneficiary_eligibility": cfda[0].beneficiary_eligibility,
                    }
                )
            else:
                toAdd.update(
                    {
                        "cfda_federal_agency": None,
                        "cfda_objectives": None,
                        "cfda_website": None,
                        "applicant_eligibility": None,
                        "beneficiary_eligibility": None,
                    }
                )

            results.append(toAdd)

        return results
