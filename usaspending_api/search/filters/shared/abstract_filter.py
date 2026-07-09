from abc import ABC, abstractmethod

from django.db.models import Q

from usaspending_api.common.exceptions import UnprocessableEntityException

CodePath = list[str]


class BaseNode(ABC):

    code: str
    ancestors: list[str]
    positive: bool
    children: list["BaseNode"]

    def __init__(
        self, code: CodePath, positive: bool, positive_codes: list[CodePath], negative_codes: list[CodePath]
    ) -> None:
        self.code = code[-1]
        self.ancestors = code[:-1]
        self.positive = positive
        self.populate_children(positive_codes, negative_codes)

    def populate_children(self, positive_codes: list[CodePath], negative_codes: list[CodePath]) -> None:
        """
        Populate child nodes based on paths that extend this node's path.

        To prevent exponential expansion while handling intermediate paths,
        we only create children for the next level that exists in the input.

        Example:
            If this node is ["A"] and positive_codes or negative_codes has ["A", "B", "C", "D"],
            we create a child for ["A", "B", "C", "D"] directly (no intermediate nodes).
            This prevents 2^N explosion while maintaining hierarchical filtering.

        """
        self.children = []
        node_path = self.ancestors + [self.code]

        # Find all paths that extends full path
        extending_positive = [c for c in positive_codes if self._extends_path(c, node_path)]
        extending_negative = [c for c in negative_codes if self._extends_path(c, node_path)]

        # Group by next level to avoid duplicates
        next_level_positive = self._get_next_level_paths(extending_positive, len(node_path))
        next_level_negative = self._get_next_level_paths(extending_negative, len(node_path))

        # Create children for unique next-level paths
        for next_path in next_level_positive:
            self.children.append(self.clone(next_path, True, positive_codes, negative_codes))

        for next_path in next_level_negative:
            self.children.append(self.clone(next_path, False, positive_codes, negative_codes))

    @staticmethod
    def _extends_path(other_path: CodePath, node_path: CodePath) -> bool:
        """
        Check if other_path extends full_path.

        Args:
            other_path: Path to check (e.g., ["A", "B", "C"])
            node_path: Current node's path (e.g., ["A"])

        Returns:
            True if other_path starts with node_path and is longer

        Example:
            _extends_path(["A", "B", "C"], ["A"]) → True
            _extends_path(["A", "B"], ["A", "B"]) → False (same length)
            _extends_path(["X", "Y"], ["A"]) → False (different prefix)
        """
        if len(other_path) <= len(node_path):
            return False
        return other_path[: len(node_path)] == node_path

    @staticmethod
    def _get_next_level_paths(paths: list[CodePath], current_depth: int) -> list[CodePath]:
        """
        Get unique paths that extend beyond current_depth.

        Deduplicates paths to prevent creating multiple nodes for the same path,
        which would cause exponential expansion.

        Args:
            paths: List of code paths to filter
            current_depth: Current node's depth in the tree

        Returns:
            List of unique paths longer than current_depth
        """

        seen = set()
        result = []
        for path in paths:
            if len(path) > current_depth:
                path_tuple = tuple(path)
                if path_tuple not in seen:
                    seen.add(path_tuple)
                    result.append(path)
        return result

    @abstractmethod
    def get_query(self) -> Q | str: ...

    @abstractmethod
    def _basic_search_unit(self) -> Q | str: ...

    @abstractmethod
    def clone(
        self, code: CodePath, positive: bool, positive_codes: list[CodePath], negative_codes: list[CodePath]
    ) -> "BaseNode": ...


class BaseHierarchicalFilter(ABC):
    """Shared logic for hierarchical code filtering (TAS, PSC, NAICS)"""

    MAX_TOTAL_CODES = 200
    MAX_TREE_DEPTH = 10

    @classmethod
    def _validate_complexity(cls, require: list[list[str]], exclude: list[list[str]]) -> None:
        total_codes = len(require) + len(exclude)
        if total_codes > cls.MAX_TOTAL_CODES:
            raise UnprocessableEntityException(f"Total codes ({total_codes}) exceeds limit ({cls.MAX_TOTAL_CODES})")

        all_codes = require + exclude
        if not all_codes:
            return

        max_depth = max(len(code) for code in all_codes)
        if max_depth > cls.MAX_TREE_DEPTH:
            raise UnprocessableEntityException(f"Code depth ({max_depth}) exceeds limit ({cls.MAX_TREE_DEPTH})")

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
