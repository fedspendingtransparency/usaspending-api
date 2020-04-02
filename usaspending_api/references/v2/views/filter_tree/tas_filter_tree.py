from usaspending_api.common.helpers.agency_logic_helpers import cfo_presentation_order
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.references.v2.views.filter_tree.filter_tree import DEFAULT_CHILDREN, Node, FilterTree
from usaspending_api.references.models import ToptierAgency
from django.db.models import Exists, OuterRef


class TASFilterTree(FilterTree):
    def raw_search(self, tiered_keys):
        if len(tiered_keys) == 0:
            return self._toptier_search()
        if len(tiered_keys) == 1:
            return self._fa_given_agency(tiered_keys[0])
        if len(tiered_keys) == 2:
            return self._tas_given_fa(tiered_keys[0], tiered_keys[1])

    def _toptier_search(self):
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

    def _fa_given_agency(self, agency):
        return FederalAccount.objects.annotate(
            has_faba=Exists(
                FinancialAccountsByAwards.objects.filter(treasury_account__federal_account=OuterRef("pk")).values("pk")
            )
        ).filter(has_faba=True, parent_toptier_agency__toptier_code=agency)

    def _tas_given_fa(self, agency, fed_account):
        return TreasuryAppropriationAccount.objects.annotate(
            has_faba=Exists(FinancialAccountsByAwards.objects.filter(treasury_account=OuterRef("pk")).values("pk"))
        ).filter(
            has_faba=True,
            federal_account__federal_account_code=fed_account,
            federal_account__parent_toptier_agency__toptier_code=agency,
        )

    def construct_node_from_raw(self, tier: int, ancestors: list, data, child_layers) -> Node:
        if tier == 0:  # A tier zero search is returning an agency dictionary
            return self._generate_agency_node(ancestors, data, child_layers)
        if tier == 1:  # A tier one search is returning a FederalAccount object
            return self._generate_federal_account_node(ancestors, data, child_layers)
        if tier == 2:  # A tier two search will be returning a TreasuryAppropriationAccount object
            return Node(
                id=data.tas_rendering_label,
                ancestors=ancestors,
                description=data.account_title,
                count=DEFAULT_CHILDREN,
                children=None,
            )

    def _generate_agency_node(self, ancestors, data, child_layers):
        if child_layers:
            raw_children = self._fa_given_agency(data["toptier_code"])
            generated_children = [
                self.construct_node_from_raw(1, ancestors + [data["toptier_code"]], elem, child_layers - 1).to_JSON()
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

    def _generate_federal_account_node(self, ancestors, data, child_layers):
        if child_layers:
            raw_children = self._tas_given_fa(ancestors[0], data.federal_account_code)
            generated_children = [
                self.construct_node_from_raw(
                    2, ancestors + [data.federal_account_code], elem, child_layers - 1
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
