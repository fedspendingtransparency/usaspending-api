from django.db.models import Q
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Cfda


class AssistanceListingViewSet(APIView):

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/references/assistance_listing.md"

    queryset = Cfda.objects.all()

    def _business_logic(self, cfda_code, filter) -> list:
        if len(str(cfda_code)) == 2 and int(cfda_code):
            self.queryset = Cfda.objects.filter(program_number__startswith=cfda_code)

        elif cfda_code is not None:
            raise InvalidParameterException(f"The assistance listing code should be two digits or not provided at all")

        all_cfda_program_numbers = self.queryset.values_list("program_number", flat=True)
        prefixes = set(cfda.split(".")[0] for cfda in all_cfda_program_numbers if "." in cfda)
        cfdas = []
        for prefix in prefixes:
            filter_query = (
                Cfda.objects.filter(Q(program_number__contains=filter) | Q(program_title__contains=filter))
                if filter is not None
                else Cfda.objects.all()
            )
            print("filter_query", filter_query)
            cfda_with_prefix = filter_query.filter(program_number__startswith=prefix)
            print("cfda_with_prefix", cfda_with_prefix)
            children = [{"code": cfda.program_number, "description": cfda.program_title} for cfda in cfda_with_prefix]
            cfdas.append(
                {
                    "code": prefix,
                    "description": None,
                    "count": len(children),
                    "children": children,
                }
            )

        return cfdas

    @cache_response()
    def get(self, request, cfda=None) -> Response:
        filter = request.query_params.get("filter", None)
        print("filter: ", filter)
        results = self._business_logic(cfda, filter)
        return Response(results)
