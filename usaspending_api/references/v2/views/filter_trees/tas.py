from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield

from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.references.v2.views.filter_trees.filter_tree import Node, FilterTree


class TASViewSet(APIView):
    """

    """

    endpoint_doc = ""

    @cache_response()
    def get(self, request: Request, tier1: str = None, tier2: str = None, tier3: str = None) -> Response:
        filter_tree = TASFilterTree()
        return Response([elem.toJSON() for elem in filter_tree.basic_search(tier1, tier2, tier3)])


class TASFilterTree(FilterTree):
    def toptier_search(self):
        return FederalAccount.objects.values("agency_identifier").distinct()

    def tier_one_search(self, agency):
        return FederalAccount.objects.filter(agency_identifier=agency)

    def tier_two_search(self, fed_account):
        return TreasuryAppropriationAccount.objects.filter(federal_account__federal_account_code=fed_account)

    def tier_three_search(self, tas_code):
        return TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_code)

    def construct_node_from_raw(self, tier: int, data) -> Node:
        if tier == 0:  # A tier zero search is returning an agency code
            return Node(name=data["agency_identifier"], ancestors=[], description="dummy", count=-1, children=[])
        if tier == 1:  # A tier one search is returning a FederalAccount object
            return Node(
                name=data.federal_account_code, ancestors=[], description=data.account_title, count=-1, children=[]
            )
        if tier == 2:  # A tier two search will be returning a TreasuryAppropriationAccount object
            return Node(
                name=data.tas_rendering_label, ancestors=[], description=data.account_title, count=-1, children=[]
            )
        if tier == 3:  # A tier three search will be returning a TreasuryAppropriationAccount object
            return Node(
                name=data.tas_rendering_label, ancestors=[], description=data.account_title, count=-1, children=[]
            )
