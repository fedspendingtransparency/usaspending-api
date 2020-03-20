from usaspending_api.common.helpers.agency_logic_helpers import cfo_presentation_order
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.references.v2.views.filter_trees.filter_tree import DEFAULT_CHILDREN, Node, FilterTree
from usaspending_api.references.models import ToptierAgency
from django.db.models import Exists, OuterRef


class TASFilterTree(FilterTree):
    def toptier_search(self):
        agency_set = (
            ToptierAgency.objects.annotate(
                has_faba=Exists(
                    FinancialAccountsByAwards.objects.filter(
                        treasury_account__federal_account__parent_toptier_agency=OuterRef("pk")
                    ).values("pk")
                )
            )
            .filter(has_faba=True)
            .values("toptier_code", "name")
        )
        agency_dictionaries = [self._dictionary_from_agency(agency) for agency in agency_set]
        cfo_sort_results = cfo_presentation_order(agency_dictionaries)
        return cfo_sort_results["cfo_agencies"] + cfo_sort_results["other_agencies"]

    def _dictionary_from_agency(self, agency):
        return {"toptier_code": agency["toptier_code"], "name": agency["name"]}

    def tier_one_search(self, agency):
        return FederalAccount.objects.filter(parent_toptier_agency__toptier_code=agency)

    def tier_two_search(self, fed_account):
        return TreasuryAppropriationAccount.objects.filter(federal_account__federal_account_code=fed_account)

    def tier_three_search(self, tas_code):
        return TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_code)

    def construct_node_from_raw(self, tier: int, ancestors: list, data, populate_children) -> Node:
        if tier == 0:  # A tier zero search is returning an agency dictionary
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
        if populate_children:
            raw_children = self.tier_one_search(data["toptier_code"])
            generated_children = [
                self.construct_node_from_raw(
                    1, ancestors + [data["toptier_code"]], elem, populate_children - 1
                ).to_JSON()
                for elem in raw_children
            ]
            count = len(generated_children)
        else:
            generated_children = None
            count = DEFAULT_CHILDREN

        return Node(
            id=data["toptier_code"],
            ancestors=ancestors,
            description=data["name"],
            count=count,
            children=generated_children,
        )

    def _generate_federal_account_node(self, ancestors, data, populate_children):
        if populate_children:
            raw_children = self.tier_two_search(data.federal_account_code)
            generated_children = [
                self.construct_node_from_raw(
                    2, ancestors + [data.federal_account_code], elem, populate_children - 1
                ).to_JSON()
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
