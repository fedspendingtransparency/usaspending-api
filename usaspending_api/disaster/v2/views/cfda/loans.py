import json

from typing import List

from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)
from usaspending_api.references.models import Cfda
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class CfdaLoansViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Query text and returns Loans of CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/cfda/loans.md"

    required_filters = ["def_codes", "query", "_loan_award_type_codes"]
    query_fields = ["cfda_title.contains", "cfda_number.contains"]
    agg_key = "cfda_agg_key"
    is_loans = True

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []

        cfda_prefetch_pks = [json.loads(bucket.get("key")).get("code") for bucket in info_buckets]
        prefetched_cfdas = Cfda.objects.filter(program_number__in=cfda_prefetch_pks)

        for bucket in info_buckets:
            info = json.loads(bucket.get("key"))
            toAdd = {
                "id": info["id"],
                "code": info["code"],
                "description": info["description"],
                "award_count": int(bucket.get("doc_count", 0)),
                "resource_link": info["url"],
                **{
                    column: get_summed_value_as_float(bucket, self.sum_column_mapping[column])
                    for column in self.sum_column_mapping
                },
            }

            cfda = [cfda for cfda in prefetched_cfdas if cfda.program_number == info.get("code")]
            if cfda:
                toAdd.update(
                    {
                        "cfda_federal_agency": cfda[0].federal_agency,
                        "cfda_objectives": cfda[0].objectives,
                        "cfda_website": cfda[0].website_address,
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
