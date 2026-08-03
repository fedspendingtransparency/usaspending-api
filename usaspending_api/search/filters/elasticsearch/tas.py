import re

from opensearchpy.helpers.query import Q as ES_Q

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.search.filters.elasticsearch.filter import QueryType, _Filter
from usaspending_api.search.filters.elasticsearch.HierarchicalFilter import HierarchicalFilter, Node
from usaspending_api.search.filters.postgres.tas import string_to_dictionary
from usaspending_api.search.filters.shared.utils import escape_regex_chars


class TasCodes(_Filter, HierarchicalFilter):
    underscore_name = "tas_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: QueryType, **options) -> ES_Q:  # noqa: ANN001
        if isinstance(filter_values, list):
            # This is a legacy usage, and will be dealt with by the other filter
            return TreasuryAccounts.generate_elasticsearch_query(filter_values, query_type)
        elif isinstance(filter_values, dict):
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise InvalidParameterException("tas_codes must be an array or object")

        return ES_Q("query_string", query=cls._query_string(require, exclude), default_field="tas_paths")

    @staticmethod
    def node(code, positive, positive_naics, negative_naics):  # noqa: ANN001
        return TASNode(code, positive, positive_naics, negative_naics)


def search_regex_of(v: str | dict) -> str:
    if isinstance(v, str):
        v = string_to_dictionary(v, "agency")

    code_lookup = {
        component: f"{component}={escape_regex_chars(v[component], char_set='lucene')}" if v.get(component) else "*"
        for component in ["agency", "faaid", "famain", "aid", "main", "ata", "sub", "bpoa", "epoa", "a"]
    }

    # This is NOT the order of elements as displayed in the tas rendering label,
    # but instead the order in the award_delta_view and transaction_delta_view
    search_regex = (
        code_lookup["agency"]
        + code_lookup["faaid"]
        + code_lookup["famain"]
        + code_lookup["aid"]
        + code_lookup["main"]
        + code_lookup["ata"]
        + code_lookup["sub"]
        + code_lookup["bpoa"]
        + code_lookup["epoa"]
        + code_lookup["a"]
    )

    # TODO: move this to a Tinyshield filter
    if not re.match(r"^(\d|\w|-|\*|=|\\)+$", search_regex):
        raise UnprocessableEntityException("Unable to parse TAS filter")

    return search_regex


class TASNode(Node):
    def _basic_search_unit(self) -> str:
        return f'({" AND ".join([search_regex_of(code) for code in self.ancestors] + [(search_regex_of(self.code))])})'

    def clone(self, code: str, positive: bool, positive_naics, negative_naics) -> Node:  # noqa: ANN001
        return TASNode(code, positive, positive_naics, negative_naics)


class TreasuryAccounts(_Filter):
    underscore_name = "treasury_account_components"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values: list, query_type: QueryType, **options) -> ES_Q:
        tas_codes_query = []

        for v in filter_values:
            code_lookup = {
                component: escape_regex_chars(v.get(component), char_set="lucene") or ".*"
                for component in ["aid", "main", "ata", "sub", "bpoa", "epoa", "a"]
            }

            search_regex = f"aid={code_lookup['aid']}main={code_lookup['main']}ata={code_lookup['ata']}sub={code_lookup['sub']}bpoa={code_lookup['bpoa']}epoa={code_lookup['epoa']}a={code_lookup['a']}"  # noqa: E501
            code_query = ES_Q("regexp", tas_components={"value": search_regex})
            tas_codes_query.append(ES_Q("bool", must=code_query))

        return ES_Q("bool", should=tas_codes_query, minimum_should_match=1)
