from abc import ABC
from django.db.models import Q, QuerySet

from usaspending_api.search.filters.shared.abstract_filter import BaseHierarchicalFilter, BaseNode, CodePath


class HierarchicalFilter(BaseHierarchicalFilter, ABC):
    @classmethod
    def _query_string(cls, queryset: QuerySet, require: list[CodePath], exclude: list[CodePath]) -> QuerySet:
        positive_nodes = [
            cls.node(code, True, require, exclude) for code in require if cls._has_no_parents(code, require + exclude)
        ]

        negative_nodes = [
            cls.node(code, False, require, exclude) for code in exclude if cls._has_no_parents(code, require + exclude)
        ]

        q = Q()
        for node in positive_nodes:
            # cancel out any require codes that also are excluded at top level
            if node.code not in [neg_node.code for neg_node in negative_nodes]:
                q |= node.get_query()
            else:
                q |= Q(pk__in=[])
        for node in negative_nodes:
            if node.children or node.code not in [pos_node.code for pos_node in positive_nodes]:
                q |= node.get_query()

        queryset = queryset.filter(q)
        return queryset


class Node(BaseNode, ABC):

    def get_query(self) -> Q:
        if self.positive:
            filter = self._basic_search_unit()
            for node in [child for child in self.children if not child.positive]:
                filter &= node.get_query()
        else:
            if [child for child in self.children if child.positive]:
                filter = Q()
                for node in [child for child in self.children if child.positive]:
                    filter |= node.get_query()
            else:
                filter = ~self._basic_search_unit()

        return filter
