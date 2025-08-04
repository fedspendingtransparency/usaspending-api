from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.elasticsearch.filter import _Filter, QueryType
from usaspending_api.search.filters.elasticsearch.HierarchicalFilter import HierarchicalFilter, Node
from elasticsearch_dsl import Q as ES_Q


class NaicsCodes(_Filter, HierarchicalFilter):
    underscore_name = "naics_codes"

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: QueryType, **options) -> ES_Q:
        # legacy functionality permits sending a single list of naics codes, which is treated as the required list
        if isinstance(filter_values, list):
            require = [cls.naics_code_to_naics_code_path(str(code)) for code in filter_values]
            exclude = []
        elif isinstance(filter_values, dict):
            require = [cls.naics_code_to_naics_code_path(str(code)) for code in filter_values.get("require") or []]
            exclude = [cls.naics_code_to_naics_code_path(str(code)) for code in filter_values.get("exclude") or []]
        else:
            raise InvalidParameterException(f"naics_codes must be an array or object")

        if [value for value in require if len(value[-1]) not in [2, 4, 6]] or [
            value for value in exclude if len(value[-1]) not in [2, 4, 6]
        ]:
            raise InvalidParameterException("naics code filtering only supported for codes with lengths of 2, 4, and 6")

        require = [code for code in require]
        exclude = [code for code in exclude]

        return ES_Q("query_string", query=cls._query_string(require, exclude), default_field="naics_code.keyword")

    @staticmethod
    def code_is_parent_of(code, other):
        return len(str(other)) == len(str(code)) + 2 and other[: len(str(code))] == str(code)

    @staticmethod
    def node(code, positive, positive_naics, negative_naics):
        return NaicsNode(code, positive, positive_naics, negative_naics)

    @staticmethod
    def naics_code_to_naics_code_path(code):
        """Special scotch-tape code to convert a single naics into a path to match the hierarchical filter API"""
        retval = []
        if len(code) > 2:
            retval.append(code[:2])
        if len(code) > 4:
            retval.append(code[:4])
        retval.append(code)
        return retval


class NaicsNode(Node):
    def _basic_search_unit(self):
        retval = f"{self.code}"
        if len(self.code) < 6:
            retval += "*"
        return retval

    def clone(self, code, positive, positive_naics, negative_naics):
        return NaicsNode(code, positive, positive_naics, negative_naics)
