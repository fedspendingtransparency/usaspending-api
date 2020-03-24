from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.elasticsearch.filters.filter import _Filter, _QueryType
from elasticsearch_dsl import Q as ES_Q


class NaicsCodes(_Filter):
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

        requires = [str(code) for code in require]
        exclude = [str(code) for code in exclude]

        return ES_Q("query_string", query=cls._query_string(requires, exclude), default_field="naics_code.keyword")

    @classmethod
    def _query_string(cls, require, exclude) -> str:
        """Generates string in proper syntax for Elasticsearch query_string attribute, given API parameters"""
        positive_codes, negative_codes = cls._order_naics_codes(require, exclude, require + exclude)

        positive_nodes = [
            _NaicsNode(code, True, positive_codes["sub"], negative_codes["sub"]) for code in positive_codes["top"]
        ]
        negative_nodes = [
            _NaicsNode(code, False, positive_codes["sub"], negative_codes["sub"]) for code in negative_codes["top"]
        ]

        positive_query = " OR ".join([node.get_query() for node in positive_nodes])
        negative_query = " AND ".join([node.get_query() for node in negative_nodes])

        if positive_query and negative_query:
            return f"{positive_query} AND {negative_query}"
        else:
            return positive_query + negative_query  # We know that exactly one is blank thanks to TinyShield

    @staticmethod
    def _order_naics_codes(requires, exclude, all_codes):
        """Seperates NAICS codes into 'top' codes (those with no higher node in either array), and 'sub' codes (those that do)."""
        postive_codes = {
            "top": [code for code in requires if len([root for root in all_codes if code[:-1].startswith(root)]) == 0]
        }
        negative_codes = {
            "top": [code for code in exclude if len([root for root in all_codes if code[:-1].startswith(root)]) == 0]
        }
        postive_codes["sub"] = [code for code in requires if code not in postive_codes["top"] + negative_codes["top"]]
        negative_codes["sub"] = [code for code in exclude if code not in postive_codes["top"] + negative_codes["top"]]

        return postive_codes, negative_codes


class _NaicsNode:
    """Represents one part of the final query, either requiring or excluding one NAICS code, with any exceptions"""

    code: str
    positive: bool
    children: list

    def __init__(self, code, positive, positive_naics, negative_naics):
        self.code = code
        self.positive = positive
        self.populate_children(positive_naics, negative_naics)

    def populate_children(self, positive_naics, negative_naics):
        self.children = []
        self._pop_children_helper(positive_naics, True, positive_naics, negative_naics)
        self._pop_children_helper(negative_naics, False, positive_naics, negative_naics)

    def _pop_children_helper(self, codes, is_positive, positive_naics, negative_naics):
        for other_code in codes:
            if len(other_code) == len(self.code) + 2 and other_code[: len(self.code)] == self.code:
                self.children.append(_NaicsNode(other_code, is_positive, positive_naics, negative_naics))

    def get_query(self):
        retval = f"{self.code}"
        if len(self.code) < 6:
            retval += "*"
        if not self.positive:
            retval = f"NOT {retval}"
        retval = f"({retval})"

        positive_child_query = " OR ".join([child.get_query() for child in self.children if child.positive])
        negative_child_query = " AND ".join([child.get_query() for child in self.children if not child.positive])
        joined_child_query = " AND ".join(query for query in [positive_child_query, negative_child_query] if query)

        if self.children:
            if self.positive:
                retval += f" AND ({joined_child_query})"
            else:
                retval += f" OR ({joined_child_query})"

        return retval
