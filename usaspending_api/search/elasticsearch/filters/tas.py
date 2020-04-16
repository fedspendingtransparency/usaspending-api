from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from elasticsearch_dsl import Q as ES_Q
from usaspending_api.search.elasticsearch.filters.filter import _Filter, _QueryType
from usaspending_api.search.elasticsearch.filters.HierarchicalFilter import HierarchicalFilter, Node


class TasCodes(_Filter, HierarchicalFilter):
    underscore_name = "tas_codes2"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: _QueryType) -> ES_Q:
        print("filter vals" + str(filter_values))
        if isinstance(filter_values, list):
            require = filter_values
            exclude = []
        elif isinstance(filter_values, dict):
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise InvalidParameterException(f"tas_codes must be an array or object")

        print("query " + cls._query_string(require, exclude))
        return ES_Q("query_string", query=cls._query_string(require, exclude), default_field="treasury_accounts")

    @staticmethod
    def node(code, positive, positive_naics, negative_naics):
        return TASNode(code, positive, positive_naics, negative_naics)

    @staticmethod
    def string_to_dictionary(string):
        if len(string.split("-")) == 1:
            return {"aid": string}
        elif len(string.split("-")) == 2:
            return FederalAccount.fa_rendering_label_to_component_dictionary(string)
        else:
            return TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary(string)


def tas_search_helper_majig(v):
    if isinstance(v, str):
        v = TasCodes.string_to_dictionary(v)

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


class TASNode(Node):
    def _basic_search_unit(self):
        return tas_search_helper_majig(self.code)

    def clone(self, code, positive, positive_naics, negative_naics):
        return TASNode(code, positive, positive_naics, negative_naics)


class _TreasuryAccounts(_Filter):
    underscore_name = "tas_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: list, query_type: _QueryType) -> ES_Q:
        tas_codes_query = []

        for v in filter_values:
            code_lookup = {
                "aid": v.get("aid", ".*"),
                "main": v.get("main", ".*"),
                "ata": v.get("ata", ".*"),
                "sub": v.get("sub", ".*"),
                "bpoa": v.get("bpoa", ".*"),
                "epoa": v.get("epoa", ".*"),
                "a": v.get("a", ".*"),
            }

            search_regex = f"aid={code_lookup['aid']}main={code_lookup['main']}ata={code_lookup['ata']}sub={code_lookup['sub']}bpoa={code_lookup['bpoa']}epoa={code_lookup['epoa']}a={code_lookup['a']}"
            code_query = ES_Q("regexp", treasury_accounts={"value": search_regex})
            tas_codes_query.append(ES_Q("bool", must=code_query))

        return ES_Q("bool", should=tas_codes_query, minimum_should_match=1)
