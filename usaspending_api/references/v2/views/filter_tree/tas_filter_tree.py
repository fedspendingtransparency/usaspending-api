from django.db.models.functions import Coalesce

from usaspending_api.common.helpers.business_logic_helpers import cfo_presentation_order, faba_with_file_D_data
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.references.v2.views.filter_tree.filter_tree import UnlinkedNode, FilterTree
from usaspending_api.references.models import ToptierAgency
from django.db.models import Exists, OuterRef, Q


class TASFilterTree(FilterTree):
    def raw_search(self, tiered_keys, child_layers, filter_string):
        if len(tiered_keys) == 0:
            if child_layers != 0:
                tier2_nodes = self.tier_2_search(tiered_keys, filter_string)
                tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)
                tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
                toptier_nodes = self.toptier_search(tier1_nodes)
                toptier_nodes = self._combine_nodes(toptier_nodes, tier1_nodes)
            return toptier_nodes
        elif len(tiered_keys) == 1:
            tier1_nodes = self.tier_1_search(tiered_keys, filter_string)
            if child_layers != 0:
                tier2_nodes = self.tier_2_search(tiered_keys, filter_string)
                tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
            return tier1_nodes
        elif len(tiered_keys) == 2:
            return self.tier_2_search(tiered_keys, filter_string)

    def _combine_nodes(self, upper_tier, lower_tier):
        for node in upper_tier:
            for node1 in lower_tier:
                if node["id"] in node1["ancestors"]:
                    node["children"].append(node1)
            sorted(node["children"], key=lambda x: x["id"])
        return upper_tier

    def toptier_search(self, tier1_nodes=None):
        filters = [
            Q(has_faba=True),
        ]
        if tier1_nodes:
            agency_ids = [node["ancestors"][0] for node in tier1_nodes]
            filters.append(Q(toptier_code__in=agency_ids))
        agency_set = (
            ToptierAgency.objects.annotate(
                has_faba=Exists(
                    faba_with_file_D_data().filter(
                        treasury_account__federal_account__parent_toptier_agency=OuterRef("pk")
                    )
                )
            )
            .filter(*filters)
            .values("toptier_code", "name", "abbreviation")
        )
        agency_dictionaries = [self._dictionary_from_agency(agency) for agency in agency_set]
        cfo_sort_results = cfo_presentation_order(agency_dictionaries)
        agencies = cfo_sort_results["cfo_agencies"] + cfo_sort_results["other_agencies"]
        return [
            {
                "id": agency["toptier_code"],
                "ancestors": [],
                "description": agency["name"] + " (" + agency["abbreviation"] + ")",
                "count": self.get_count([], agency["toptier_code"]),
                "children": [],
            }
            for agency in agencies
        ]

    def tier_3_search(self, ancestor_array, filter_string) -> list:
        return []

    def tier_2_search(self, ancestor_array, filter_string,) -> list:
        filters = [
            Q(has_faba=True),
        ]
        if len(ancestor_array) > 0:
            agency = ancestor_array[0]
            filters.append(Q(agency=agency))
        if len(ancestor_array) > 1:
            fed_account = ancestor_array[1]
            filters.append(Q(federal_account__federal_account_code=fed_account))
        if filter_string:
            filters.append(
                Q(Q(tas_rendering_label__icontains=filter_string) | Q(account_title__icontains=filter_string))
            )
        data = TreasuryAppropriationAccount.objects.annotate(
            has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk"))),
            agency=Coalesce("federal_account__parent_toptier_agency__toptier_code", "agency_id"),
        ).filter(*filters)
        retval = []
        for item in data:
            ancestor_array = [item.agency, item.agency_id + "-" + item.main_account_code]
            retval.append(
                {
                    "id": item.tas_rendering_label,
                    "ancestors": ancestor_array,
                    "description": item.account_title,
                    "count": 0,
                    "children": [],
                }
            )
        return retval

    def tier_1_search(self, ancestor_array, filter_string, tier2_nodes=None) -> list:
        filters = [
            Q(has_faba=True),
        ]
        if tier2_nodes:
            fed_account = [node["ancestors"][1] for node in tier2_nodes]
            filters.append(Q(federal_account_code__in=fed_account))
        if len(ancestor_array):
            agency = ancestor_array[0]
            filters.append(Q(agency=agency))
        if filter_string:
            filters.append(
                Q(Q(federal_account_code__icontains=filter_string) | Q(account_title__icontains=filter_string))
            )
        data = FederalAccount.objects.annotate(
            has_faba=Exists(faba_with_file_D_data().filter(treasury_account__federal_account=OuterRef("pk"))),
            agency=Coalesce("parent_toptier_agency__toptier_code", "agency_identifier"),
        ).filter(*filters)
        retval = []
        for item in data:
            ancestor_array = [item.agency]
            retval.append(
                {
                    "id": item.federal_account_code,
                    "ancestors": ancestor_array,
                    "description": item.account_title,
                    "count": self.get_count(ancestor_array, item.federal_account_code),
                    "children": [],
                }
            )
        return retval

    def _dictionary_from_agency(self, agency):
        return {"toptier_code": agency["toptier_code"], "name": agency["name"], "abbreviation": agency["abbreviation"]}

    def _fa_given_agency(self, agency):
        filters = [Q(has_faba=True), Q(parent_toptier_agency__toptier_code=agency)]
        return FederalAccount.objects.annotate(
            has_faba=Exists(faba_with_file_D_data().filter(treasury_account__federal_account=OuterRef("pk")))
        ).filter(*filters)

    def _tas_given_fa(self, agency, fed_account):
        filters = [Q(has_faba=True), Q(main_account_code=fed_account), Q(agency_id=agency)]
        return TreasuryAppropriationAccount.objects.annotate(
            has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk")))
        ).filter(*filters)

    def unlinked_node_from_data(self, ancestors: list, data) -> UnlinkedNode:
        if len(ancestors) == 0:  # A tier zero search is returning an agency dictionary
            return self._generate_agency_node(ancestors, data)
        if len(ancestors) == 1:  # A tier one search is returning a FederalAccount object
            return self._generate_federal_account_node(ancestors, data)
        if len(ancestors) == 2:  # A tier two search will be returning a TreasuryAppropriationAccount object
            return UnlinkedNode(id=data.tas_rendering_label, ancestors=ancestors, description=data.account_title)

    def _generate_agency_node(self, ancestors, data):
        return UnlinkedNode(
            id=data["toptier_code"], ancestors=ancestors, description=f"{data['name']} ({data['abbreviation']})"
        )

    def _generate_federal_account_node(self, ancestors, data):
        return UnlinkedNode(id=data.federal_account_code, ancestors=ancestors, description=data.account_title)

    def get_count(self, tiered_keys: list, id) -> int:
        if len(tiered_keys) == 0:
            taa_filters = [
                Q(has_faba=True),
                Q(agency_id=id),
            ]
            taa_count = (
                TreasuryAppropriationAccount.objects.annotate(
                    has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk")))
                )
                .filter(*taa_filters)
                .count()
            )
            return taa_count
        if len(tiered_keys) == 1:
            x = id.split("-")
            if len(x) < 2:
                x = [tiered_keys[0], id]
            taa_filters = [
                Q(has_faba=True),
                Q(agency_id=x[0]),
                Q(main_account_code=x[1]),
            ]
            taa_count = (
                TreasuryAppropriationAccount.objects.annotate(
                    has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk")))
                )
                .filter(*taa_filters)
                .count()
            )
            return taa_count
        if len(tiered_keys) == 2:
            return 1
        return 0
