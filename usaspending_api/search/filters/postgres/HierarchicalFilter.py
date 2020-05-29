from abc import abstractmethod
from django.db.models import Q


class HierarchicalFilter:
    @classmethod
    def _query_string(cls, queryset, require, exclude):
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

    @classmethod
    def _has_no_parents(cls, code, other_codes):
        return not len([match for match in other_codes if cls.code_is_parent_of(match, code)])

    @staticmethod
    def code_is_parent_of(code, other):
        return other[: len(code)] == code and len(code) < len(other)

    @staticmethod
    @abstractmethod
    def node(code, positive, positive_codes, negative_codes):
        pass


class Node:
    """Represents one part of the final query, either requiring or excluding one code, with any exceptions"""

    code: str
    ancestors: list
    positive: bool
    children: list

    def __init__(self, code, positive, positive_codes, negative_codes):
        self.code = code[-1]
        self.ancestors = code[:-1]
        self.positive = positive
        self.populate_children(positive_codes, negative_codes)

    def populate_children(self, positive_codes, negative_codes):
        self.children = []
        self._pop_children_helper(positive_codes, True, positive_codes, negative_codes)
        self._pop_children_helper(negative_codes, False, positive_codes, negative_codes)

    def _pop_children_helper(self, codes, is_positive, positive_codes, negative_codes):
        for other_code in codes:
            if self.is_parent_of(other_code):
                self.children.append(self.clone(other_code, is_positive, positive_codes, negative_codes))

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

    @abstractmethod
    def _basic_search_unit(self) -> dict:
        pass

    def is_parent_of(self, other_path):
        return self.code in other_path[:-1]

    @abstractmethod
    def clone(self, code, positive, positive_codes, negative_codes):
        pass
