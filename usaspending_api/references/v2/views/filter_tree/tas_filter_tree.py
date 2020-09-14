from usaspending_api.common.helpers.business_logic_helpers import cfo_presentation_order, faba_with_file_D_data
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.references.v2.views.filter_tree.filter_tree import UnlinkedNode, FilterTree
from usaspending_api.references.models import ToptierAgency
from django.db.models import Exists, OuterRef, Q


class TASFilterTree(FilterTree):
    def raw_search(self, tiered_keys):
        if len(tiered_keys) == 0:
            return self._toptier_search()
        if len(tiered_keys) == 1:
            return self._fa_given_agency(tiered_keys[0])
        if len(tiered_keys) == 2:
            return self._tas_given_fa(tiered_keys[0], tiered_keys[1])
        return []

    def _toptier_search(self):
        agency_set = (
            ToptierAgency.objects.annotate(
                has_faba=Exists(
                    faba_with_file_D_data().filter(
                        treasury_account__federal_account__parent_toptier_agency=OuterRef("pk")
                    )
                )
            )
            .filter(has_faba=True)
            .values("toptier_code", "name", "abbreviation")
        )
        agency_dictionaries = [self._dictionary_from_agency(agency) for agency in agency_set]
        cfo_sort_results = cfo_presentation_order(agency_dictionaries)
        return cfo_sort_results["cfo_agencies"] + cfo_sort_results["other_agencies"]

    def _dictionary_from_agency(self, agency):
        return {"toptier_code": agency["toptier_code"], "name": agency["name"], "abbreviation": agency["abbreviation"]}

    def _fa_given_agency(self, agency):
        filters = [Q(has_faba=True), Q(parent_toptier_agency__toptier_code=agency)]
        return FederalAccount.objects.annotate(
            has_faba=Exists(faba_with_file_D_data().filter(treasury_account__federal_account=OuterRef("pk")))
        ).filter(*filters)

    def _tas_given_fa(self, agency, fed_account):
        filters = [
            Q(has_faba=True),
            Q(main_account_code=fed_account),
            Q(agency_id=agency),
        ]
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
            print(x)
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
            print(taa_count)
            return taa_count
        if len(tiered_keys) == 2:
            return 1
        return 0
