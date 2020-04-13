from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from elasticsearch_dsl import Q as ES_Q
from usaspending_api.search.elasticsearch.filters.filter import _Filter, _QueryType
from usaspending_api.search.elasticsearch.filters.HierarchicalFilter import HierarchicalFilter, Node


class TasCodes(_Filter, HierarchicalFilter):
    underscore_name = "tas_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: _QueryType) -> ES_Q:

        if isinstance(filter_values, list):
            require = filter_values
            exclude = []
        elif isinstance(filter_values, dict):
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise InvalidParameterException(f"tas_codes must be an array or object")

        return ES_Q("query_string", query=cls._query_string(require, exclude), default_field="treasury_accounts")

    @staticmethod
    def node(code, positive, positive_naics, negative_naics):
        return TASNode(code, positive, positive_naics, negative_naics)


class TASNode(Node):
    def _basic_search_unit(self):
        v = self.code
        if isinstance(self.code, str):
            if len(self.code.split("-")) == 1:
                v = {"aid": self.code}
            elif len(self.code.split("-")) == 2:
                v = FederalAccount.fa_rendering_label_to_component_dictionary(v)
            else:
                v = TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary(v)

        code_lookup = {
            "aid": f"aid={v['aid']}" if v.get("aid") else "*",
            "main": f"main={v['main']}" if v.get("main") else "*",
            "ata": f"ata={v['ata']}" if v.get("ata") else "*",
            "sub": f"sub={v['sub']}" if v.get("sub") else "*",
            "bpoa": f"bpoa={v['bpoa']}" if v.get("bpoa") else "*",
            "epoa": f"epoa={v['epoa']}" if v.get("epoa") else "*",
            "a": f"a={v['a']}" if v.get("a") else "*",
        }

        # This is NOT the order of elements as displayed in the tas rendering label, but instead the order in the award_delta_view and transaction_delta_view
        search_regex = (
            code_lookup["aid"]
            + code_lookup["main"]
            + code_lookup["ata"]
            + code_lookup["sub"]
            + code_lookup["bpoa"]
            + code_lookup["epoa"]
            + code_lookup["a"]
        )

        return search_regex

    def is_parent_of(self, other_code):
        return False

    def is_toptier(self):
        return True

    def _self_replicate(self, code, positive, positive_naics, negative_naics):
        return TASNode(code, positive, positive_naics, negative_naics)
