from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield

from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.common.helpers.agency_logic_helpers import agency_from_identifiers
from usaspending_api.references.v2.views.filter_trees.filter_tree import DEFAULT_CHILDREN, Node, FilterTree


class TASViewSet(APIView):
    """

    """

    endpoint_doc = ""

    def _parse_and_validate(self, request):

        data = {"depth": request.get("depth") or 1}
        models = [
            {"key": "depth", "name": "depth", "type": "integer", "allow_nulls": True, "default": 1, "optional": True}
        ]
        return TinyShield(models).block(data)

    @cache_response()
    def get(self, request: Request, tier1: str = None, tier2: str = None, tier3: str = None) -> Response:
        request_values = self._parse_and_validate(request.GET)

        filter_tree = TASFilterTree()
        return Response([elem.toJSON() for elem in filter_tree.search(tier1, tier2, tier3, request_values["depth"])])


class TASFilterTree(FilterTree):
    def toptier_search(self):
        return TreasuryAppropriationAccount.objects.values("fr_entity_code", "agency_id").distinct()

    def tier_one_search(self, agency):
        return FederalAccount.objects.filter(agency_identifier=agency)

    def tier_two_search(self, fed_account):
        return TreasuryAppropriationAccount.objects.filter(federal_account__federal_account_code=fed_account)

    def tier_three_search(self, tas_code):
        return TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_code)

    def construct_node_from_raw(self, tier: int, ancestors: list, data, populate_children) -> Node:
        if tier == 0:  # A tier zero search is returning an agency code
            return self._generate_agency_node(ancestors, data, populate_children)
        if tier == 1:  # A tier one search is returning a FederalAccount object
            return self._generate_federal_account_node(ancestors, data, populate_children)
        if tier == 2 or tier == 3:  # A tier two or three search will be returning a TreasuryAppropriationAccount object
            return Node(
                id=data.tas_rendering_label,
                ancestors=ancestors,
                description=data.account_title,
                count=DEFAULT_CHILDREN,
                children=None,
            )

    def _generate_agency_node(self, ancestors, data, populate_children):
        matching_agency = agency_from_identifiers(data["agency_id"], data["fr_entity_code"])
        if matching_agency:
            if populate_children:
                raw_children = self.tier_one_search(matching_agency.toptier_code)
                generated_children = [
                    self.construct_node_from_raw(
                        1, ancestors + [data["agency_id"]], elem, populate_children - 1
                    ).toJSON()
                    for elem in raw_children
                ]
                count = len(generated_children)
            else:
                generated_children = None
                count = DEFAULT_CHILDREN

            return Node(
                id=data["agency_id"],
                ancestors=ancestors,
                description=matching_agency.name,
                count=count,
                children=generated_children,
            )
        else:
            return Node(
                id="NOT FOUND",
                ancestors=ancestors,
                description=f"Failed to find {data['agency_id']},{data['fr_entity_code']}",
                count=-1,
                children=None,
            )

    def _generate_federal_account_node(self, ancestors, data, populate_children):
        if populate_children:
            raw_children = self.tier_two_search(data.federal_account_code)
            generated_children = [
                self.construct_node_from_raw(
                    2, ancestors + [data.federal_account_code], elem, populate_children - 1
                ).toJSON()
                for elem in raw_children
            ]
            count = len(generated_children)
        else:
            generated_children = None
            count = DEFAULT_CHILDREN

        return Node(
            id=data.federal_account_code,
            ancestors=ancestors,
            description=data.account_title,
            count=count,
            children=generated_children,
        )
