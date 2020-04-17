from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.awards.v2.lookups.lookups import (
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
)


class AwardTypeGroups(APIView):
    """
    This route returns a JSON object describing the award type groupings.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/award_types.md"

    def get(self, request, format=None):
        return Response(
            {
                "contracts": contract_type_mapping,
                "loans": loan_type_mapping,
                "idvs": idv_type_mapping,
                "grants": grant_type_mapping,
                "other_financial_assistance": other_type_mapping,
                "direct_payments": direct_payment_type_mapping,
            }
        )
