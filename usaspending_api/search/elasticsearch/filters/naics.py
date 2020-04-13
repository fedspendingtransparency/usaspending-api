from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.elasticsearch.filters.filter import _Filter, _QueryType
from usaspending_api.search.elasticsearch.filters.HierarchicalFilter import HierarchicalFilter, Node
from elasticsearch_dsl import Q as ES_Q


class NaicsCodes(_Filter, HierarchicalFilter):
    underscore_name = "naics_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: _QueryType) -> ES_Q:
        # legacy functionality permits sending a single list of naics codes, which is treated as the required list
        if isinstance(filter_values, list):
            require = filter_values
            exclude = []
        elif isinstance(filter_values, dict):
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise InvalidParameterException(f"naics_codes must be an array or object")

        if [value for value in require if len(str(value)) not in [2, 4, 6]] or [
            value for value in exclude if len(str(value)) not in [2, 4, 6]
        ]:
            raise InvalidParameterException("naics code filtering only supported for codes with lengths of 2, 4, and 6")

        require = [str(code) for code in require]
        exclude = [str(code) for code in exclude]

        print(
            "query: "
            + str(
                ES_Q(
                    "query_string", query=cls._query_string(require, exclude), default_field="naics_code.keyword"
                ).to_dict()
            )
        )
        return ES_Q("query_string", query=cls._query_string(require, exclude), default_field="naics_code.keyword")

    @staticmethod
    def node(code, positive, positive_naics, negative_naics):
        return NaicsNode(code, positive, positive_naics, negative_naics)


class NaicsNode(Node):
    def _basic_search_unit(self):
        retval = f"{self.code}"
        if len(self.code) < 6:
            retval += "*"
        return retval

    def is_parent_of(self, other_code):
        return len(str(other_code)) == len(str(self.code)) + 2 and other_code[: len(str(self.code))] == str(self.code)

    def is_toptier(self):
        return len(str(self.code)) == 2

    def _self_replicate(self, code, positive, positive_naics, negative_naics):
        return NaicsNode(code, positive, positive_naics, negative_naics)
