from abc import ABC, abstractmethod

from django.db.models import Q

CodePath = list[str]


class BaseNode(ABC):

    code: str
    ancestors: list
    positive: bool
    children: list

    def __init__(
        self, code: CodePath, positive: bool, positive_codes: list[CodePath], negative_codes: list[CodePath]
    ) -> None:
        self.code = code[-1]
        self.ancestors = code[:-1]
        self.positive = positive
        self.populate_children(positive_codes, negative_codes)

    def populate_children(self, positive_codes: list[CodePath], negative_codes: list[CodePath]) -> None:
        self.children = []

        direct_positive = [c for c in positive_codes if self.is_direct_child(c)]
        direct_negative = [c for c in negative_codes if self.is_direct_child(c)]

        self._pop_children_helper(direct_positive, True, positive_codes, negative_codes)
        self._pop_children_helper(direct_negative, False, positive_codes, negative_codes)

    def _pop_children_helper(
        self,
        codes: list[CodePath],
        is_positive: bool,
        positive_codes: list[CodePath],
        negative_codes: list[CodePath],
    ) -> None:
        for other_code in codes:
            if self.is_parent_of(other_code):
                self.children.append(self.clone(other_code, is_positive, positive_codes, negative_codes))

    @abstractmethod
    def get_query(self) -> Q | str: ...

    @abstractmethod
    def _basic_search_unit(self) -> Q | str: ...

    def is_direct_child(self, other_path: CodePath) -> bool:
        if len(other_path) != len(self.ancestors) + 2:
            return False
        return other_path[:-1] == self.ancestors + [self.code]

    def is_parent_of(self, other_path: CodePath) -> bool:
        return self.code in other_path[:-1]

    @abstractmethod
    def clone(
        self, code: CodePath, positive: bool, positive_codes: list[CodePath], negative_codes: list[CodePath]
    ) -> "BaseNode": ...


class BaseHierarchicalFilter(ABC):
    """Shared logic for hierarchical code filtering (TAS, PSC, NAICS)"""

    MAX_TOTAL_CODES = 100
    MAX_TREE_DEPTH = 10
    MAX_CHAIN_LENGTH = 15

    @classmethod
    def _validate_complexity(cls, require: list[list[str]], exclude: list[list[str]]) -> None:
        total_codes = len(require) + len(exclude)
        if total_codes > cls.MAX_TOTAL_CODES:
            raise ValueError(f"Total codes ({total_codes}) exceeds limit ({cls.MAX_TOTAL_CODES})")

        all_codes = require + exclude
        if not all_codes:
            return

        max_depth = max(len(code) for code in all_codes)
        if max_depth > cls.MAX_TREE_DEPTH:
            raise ValueError(f"Code depth ({max_depth}) exceeds limit ({cls.MAX_TREE_DEPTH})")

        sorted_codes = sorted(all_codes, key=len)
        chain_length = 1
        for i in range(1, len(sorted_codes)):
            if cls.code_is_parent_of(sorted_codes[i - 1], sorted_codes[i]):
                chain_length += 1
                if chain_length > cls.MAX_CHAIN_LENGTH:
                    raise ValueError(
                        f"Detected nested chain of {chain_length} paths that would "
                        f"cause exponential expansion (limit: {cls.MAX_CHAIN_LENGTH})"
                    )
            else:
                chain_length = 1

    @classmethod
    def _has_no_parents(cls, code: list[str], other_codes: list[list[str]]) -> bool:
        return not len([match for match in other_codes if cls.code_is_parent_of(match, code)])

    @staticmethod
    def code_is_parent_of(code: list[str], other: list[str]) -> bool:
        return other[: len(code)] == code and len(code) < len(other)

    @staticmethod
    @abstractmethod
    def node(
        code: list[str], positive: bool, positive_codes: list[list[str]], negative_codes: list[list[str]]
    ) -> BaseNode: ...
