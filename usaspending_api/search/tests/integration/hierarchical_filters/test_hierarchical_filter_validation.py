import pytest

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.search.filters.shared.abstract_filter import BaseHierarchicalFilter


class MockHierarchicalFilter(BaseHierarchicalFilter):
    """Concrete implementation for testing"""

    @staticmethod
    def node(code, positive, positive_codes, negative_codes):
        pass


class TestValidateComplexity:
    """Test _validate_complexity protection"""

    def test_accepts_empty_input(self):
        """Should accept empty require and exclude lists"""
        MockHierarchicalFilter._validate_complexity([], [])
        # No exception = pass

    def test_accepts_valid_simple_codes(self):
        """Should accept normal usage with few codes"""
        require = [["A"], ["B"], ["C"]]
        exclude = [["D"]]

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_accepts_valid_hierarchical_codes(self):
        """Should accept valid hierarchical structures"""
        require = [
            ["091"],
            ["091", "091-0800"],
            ["092"],
            ["092", "092-1000"],
        ]
        exclude = [["091", "091-0800", "091-0800-001"]]

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_accepts_max_allowed_codes(self):
        """Should accept exactly MAX_TOTAL_CODES (100)"""
        require = [[f"CODE{i}"] for i in range(120)]
        exclude = [[f"CODE{i}"] for i in range(120, 200)]  # Total = 200

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_rejects_too_many_codes(self):
        """Should reject when total codes exceed MAX_TOTAL_CODES"""
        require = [[f"CODE{i}"] for i in range(120)]
        exclude = [[f"CODE{i}"] for i in range(120, 201)]  # Total = 201

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "Total codes (201) exceeds limit (200)" in str(exc_info.value)

    def test_rejects_way_too_many_codes(self):
        """Should reject obvious DoS attempts with many codes"""
        require = [[f"CODE{i}"] for i in range(500)]
        exclude = []

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "exceeds limit (200)" in str(exc_info.value)

    def test_accepts_max_allowed_depth(self):
        """Should accept paths at exactly MAX_TREE_DEPTH (10)"""
        require = [["A"] + [f"B{i}" for i in range(9)]]  # 10 levels total
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_rejects_too_deep_paths(self):
        """Should reject paths deeper than MAX_TREE_DEPTH"""
        require = [["A"] + [f"B{i}" for i in range(10)]]  # 11 levels deep
        exclude = []

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "Code depth (11) exceeds limit (10)" in str(exc_info.value)

    def test_rejects_extremely_deep_paths(self):
        """Should reject very deep nesting"""
        require = [["A"] + [f"B{i}" for i in range(50)]]  # 51 levels deep
        exclude = []

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "exceeds limit (10)" in str(exc_info.value)

    def test_accepts_multiple_independent_chains(self):
        """Should accept multiple short chains that don't nest"""
        require = [
            ["A"],
            ["A", "B"],
            ["A", "B", "C"],  # Chain 1: length 3
            ["X"],
            ["X", "Y"],
            ["X", "Y", "Z"],  # Chain 2: length 3
            ["P"],
            ["P", "Q"],  # Chain 3: length 2
        ]
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass (chains are independent)

    def test_accepts_branching_hierarchies(self):
        """Should accept tree structures that branch (not monotone)"""
        require = [
            ["A"],
            ["A", "B"],
            ["A", "C"],  # Branches from A
            ["A", "B", "D"],
            ["A", "B", "E"],  # Branches from A->B
        ]
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass (branching prevents monotone chain)

    def test_chain_detection_resets_on_branch(self):
        """Chain counter should reset when hierarchy branches"""
        require = [
            ["A"],
            ["A", "B"],
            ["A", "B", "C"],
            ["A", "B", "C", "D"],  # Chain of 4
            ["X"],  # New chain starts
            ["X", "Y"],
            ["X", "Y", "Z"],  # Another chain of 3
        ]
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass (each chain is short)

    def test_validates_exclude_codes_too(self):
        """Should validate exclude codes, not just require"""
        require = []
        exclude = [["A"] + [f"B{i}" for i in range(j)] for j in range(11)]

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "Code depth (11) exceeds limit (10)" in str(exc_info.value)

    def test_validates_combined_require_and_exclude(self):
        """Should validate require + exclude together for total count"""
        require = [[f"REQ{i}"] for i in range(120)]
        exclude = [[f"EXC{i}"] for i in range(100)]  # Total = 220

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        assert "Total codes (220) exceeds limit (200)" in str(exc_info.value)

    def test_chain_detection_across_require_and_exclude(self):
        """Should detect depth violations across require and exclude lists"""
        # Create paths that exceed depth limit
        require = [["A"] + [f"B{i}" for i in range(j)] for j in range(6)]  # Depth 6
        exclude = [["A"] + [f"B{i}" for i in range(j)] for j in range(6, 12)]  # Depth 12

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        # Depth limit triggers (12 > 10)
        assert "Code depth (12) exceeds limit (10)" in str(exc_info.value)

    def test_real_world_psc_codes(self):
        """Should accept realistic PSC code usage"""
        require = [
            ["Service", "B"],
            ["Service", "B", "B5"],
            ["Service", "C"],
            ["Product", "10"],
        ]
        exclude = [
            ["Service", "B", "B5", "B502"],
        ]

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_real_world_tas_codes(self):
        """Should accept realistic TAS code usage"""
        require = [
            ["091"],
            ["091", "091-0800"],
            ["092"],
            ["092", "092-1000"],
        ]
        exclude = [
            ["091", "091-0800", "091-0800-001"],
        ]

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_edge_case_single_code(self):
        """Should accept single code"""
        require = [["A"]]
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_edge_case_all_root_codes(self):
        """Should accept many root-level codes (no nesting)"""
        require = [[f"CODE{i}"] for i in range(50)]
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass

    def test_boundary_exactly_at_limits(self):
        """Should accept input exactly at all limits"""
        # 100 codes, depth 10
        require = [["A"] + [f"B{i}" for i in range(j)] for j in range(10)]  # Chain of 10, depth 10
        # Add more codes to reach 200 total (but keep them shallow)
        require.extend([[f"X{i}"] for i in range(190)])
        exclude = []

        MockHierarchicalFilter._validate_complexity(require, exclude)
        # No exception = pass


class TestValidateComplexityErrorMessages:
    """Test that error messages are helpful"""

    def test_total_codes_error_shows_actual_and_limit(self):
        """Error should show both actual count and limit"""
        require = [[f"CODE{i}"] for i in range(201)]
        exclude = []

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        error_msg = str(exc_info.value)
        assert "201" in error_msg  # Actual count
        assert "200" in error_msg  # Limit

    def test_depth_error_shows_actual_and_limit(self):
        """Error should show both actual depth and limit"""
        require = [["A"] + [f"B{i}" for i in range(20)]]  # 21 levels
        exclude = []

        with pytest.raises(UnprocessableEntityException) as exc_info:
            MockHierarchicalFilter._validate_complexity(require, exclude)

        error_msg = str(exc_info.value)
        assert "21" in error_msg  # Actual depth
        assert "10" in error_msg  # Limit
