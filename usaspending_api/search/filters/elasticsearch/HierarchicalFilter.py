from abc import ABC

from usaspending_api.search.filters.shared.abstract_filter import BaseHierarchicalFilter, BaseNode, CodePath


class HierarchicalFilter(BaseHierarchicalFilter, ABC):
    @classmethod
    def _query_string(cls, require: list[CodePath], exclude: list[CodePath]) -> str:
        """Generates string in proper syntax for Elasticsearch query_string attribute, given API parameters"""
        cls._validate_complexity(require, exclude)
        positive_nodes = [
            cls.node(code, True, require, exclude) for code in require if cls._has_no_parents(code, require + exclude)
        ]

        negative_nodes = [
            cls.node(code, False, require, exclude) for code in exclude if cls._has_no_parents(code, require + exclude)
        ]

        positive_query = " OR ".join(
            [
                node.get_query()
                for node in positive_nodes
                if node.code not in [neg_node.code for neg_node in negative_nodes]
            ]
        )
        negative_query = " AND ".join(
            [
                node.get_query()
                for node in negative_nodes
                if (node.children or not positive_nodes)
                and node.code not in [pos_node.code for pos_node in positive_nodes]
            ]
        )

        if positive_query and negative_query:
            return f"({positive_query}) OR ({negative_query})"
        if not positive_query and not negative_query:
            return "NOT *"  # return nothing
        else:
            return positive_query + negative_query  # We know that exactly one is blank thanks to TinyShield


class Node(BaseNode, ABC):
    """Represents one part of the final query, either requiring or excluding one code, with any exceptions"""

    def get_query(self) -> str:
        retval = self._basic_search_unit()
        if self.positive:
            retval = f"({retval})"
            negative_child_query = " AND ".join([child.get_query() for child in self.children if not child.positive])
            if negative_child_query:
                negative_child_query = f"({negative_child_query})"
                retval = f"({retval} AND ({negative_child_query}))"
        else:
            if [child for child in self.children if child.positive]:
                positive_child_query = " OR ".join([child.get_query() for child in self.children if child.positive])
                retval = f"({positive_child_query})"
            else:
                retval = f"(NOT {retval})"

        return retval
