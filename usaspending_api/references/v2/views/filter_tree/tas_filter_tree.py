from usaspending_api.common.helpers.business_logic_helpers import cfo_presentation_order, faba_with_file_D_data
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.v2.views.filter_tree.filter_tree import FilterTree
from django.db.models import Exists, OuterRef, Q, F, Count


class TASFilterTree(FilterTree):
    def raw_search(self, tiered_keys, child_layers, filter_string):
        if len(tiered_keys) == 0:
            if child_layers != 0:
                if child_layers == 2 or child_layers == -1:
                    tier2_nodes = self.tier_2_search(tiered_keys, filter_string)
                    tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)
                    tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
                else:
                    tier1_nodes = self.tier_1_search(tiered_keys, filter_string)
                toptier_nodes = self.toptier_search(filter_string, tier1_nodes)
                toptier_nodes = self._combine_nodes(toptier_nodes, tier1_nodes)
            else:
                toptier_nodes = self.toptier_search(filter_string)
            return toptier_nodes
        elif len(tiered_keys) == 1:
            if child_layers != 0:
                tier2_nodes = self.tier_2_search(tiered_keys, filter_string)
                tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)
                tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
            else:
                tier1_nodes = self.tier_1_search(tiered_keys, filter_string)
            return tier1_nodes
        elif len(tiered_keys) == 2:
            return self.tier_2_search(tiered_keys, filter_string)

    def _combine_nodes(self, upper_tier, lower_tier):
        for upper_node in upper_tier:
            children = []
            for lower_node in lower_tier:
                if upper_node["id"] in lower_node["ancestors"]:
                    children.append(lower_node)
            upper_node["children"] = sorted(children, key=lambda x: x["id"])
        return upper_tier

    def tier_2_search(self, ancestor_array, filter_string) -> list:
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
            agency=F("federal_account__parent_toptier_agency__toptier_code"),
            fed_account=F("federal_account__federal_account_code"),
        ).filter(*filters)
        retval = []
        for item in data:
            ancestor_array = [item.agency, item.fed_account]
            retval.append(
                {
                    "id": item.tas_rendering_label,
                    "ancestors": ancestor_array,
                    "description": item.account_title,
                    "count": 0,
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def tier_1_search(self, ancestor_array, filter_string, tier2_nodes=None) -> list:
        filters = [Q(has_faba=True)]
        query = Q()
        if tier2_nodes:
            fed_account = [node["ancestors"][1] for node in tier2_nodes]
            query |= Q(federal_account__federal_account_code__in=fed_account)
        if len(ancestor_array):
            agency = ancestor_array[0]
            filters.append(Q(agency=agency))
        if filter_string:
            query |= Q(
                Q(federal_account__federal_account_code__icontains=filter_string)
                | Q(federal_account__account_title__icontains=filter_string)
            )
        if query != Q():
            filters.append(query)
        data = (
            TreasuryAppropriationAccount.objects.annotate(
                has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk"))),
            )
            .annotate(agency=F("federal_account__parent_toptier_agency__toptier_code"))
            .filter(*filters)
            .values("federal_account__federal_account_code", "federal_account__account_title", "agency")
            .annotate(count=Count("treasury_account_identifier"))
        )

        retval = [
            {
                "id": item["federal_account__federal_account_code"],
                "ancestors": [item["agency"]],
                "description": item["federal_account__account_title"],
                "count": item["count"],
                "children": None,
            }
            for item in data
        ]

        return sorted(retval, key=lambda x: x["id"])

    def toptier_search(self, filter_string=None, tier1_nodes=None):
        filters = [Q(has_faba=True)]
        query = Q()
        if tier1_nodes:
            agency_ids = [node["ancestors"][0] for node in tier1_nodes]
            query |= Q(federal_account__parent_toptier_agency__toptier_code__in=agency_ids)
        if filter_string:
            query |= Q(
                Q(federal_account__parent_toptier_agency__toptier_code__icontains=filter_string)
                | Q(federal_account__parent_toptier_agency__name__icontains=filter_string)
            )
        if query != Q():
            filters.append(query)
        agency_set = (
            TreasuryAppropriationAccount.objects.annotate(
                has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk"))),
            )
            .filter(*filters)
            .values(
                "federal_account__parent_toptier_agency__toptier_code",
                "federal_account__parent_toptier_agency__name",
                "federal_account__parent_toptier_agency__abbreviation",
            )
            .annotate(count=Count("treasury_account_identifier"))
        )
        agency_dictionaries = [self._dictionary_from_agency(agency) for agency in agency_set]
        cfo_sort_results = cfo_presentation_order(agency_dictionaries)
        agencies = cfo_sort_results["cfo_agencies"] + cfo_sort_results["other_agencies"]
        return [
            {
                "id": agency["toptier_code"],
                "ancestors": [],
                "description": f"{agency['name']} ({agency['abbreviation']})",
                "count": agency["count"],
                "children": None,
            }
            for agency in agencies
        ]

    def _dictionary_from_agency(self, agency):
        return {
            "toptier_code": agency["federal_account__parent_toptier_agency__toptier_code"],
            "name": agency["federal_account__parent_toptier_agency__name"],
            "abbreviation": agency["federal_account__parent_toptier_agency__abbreviation"],
            "count": agency["count"],
        }
