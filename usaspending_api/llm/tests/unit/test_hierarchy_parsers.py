import pytest

from usaspending_api.llm.tools.helpers import hierarchy_parsers


class TestNaicsAncestors:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("336411", {"33", "3364"}),
            ("3364", {"33"}),
            ("33", set()),
            ("12", set()),
        ],
    )
    def test_ancestors(self, code, expected):
        assert hierarchy_parsers.get_naics_ancestors(code) == expected


class TestNaicsParent:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("336411", "3364"),
            ("3364", "33"),
            ("33", None),
        ],
    )
    def test_parent(self, code, expected):
        assert hierarchy_parsers.get_naics_parent(code) == expected


class TestPscAncestors:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("1055", {"10"}),
            ("10", set()),
            ("AG", {"A"}),
            ("AG10", {"A", "AG"}),
            ("A", set()),
        ],
    )
    def test_ancestors(self, code, expected):
        assert hierarchy_parsers.get_psc_ancestors(code) == expected


class TestPscParent:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("1055", "10"),
            ("10", None),
            ("AG", "A"),
            ("AG10", "AG"),
            ("A", None),
        ],
    )
    def test_parent(self, code, expected):
        assert hierarchy_parsers.get_psc_parent(code) == expected


class TestCfdaAncestors:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("15.619", {"15"}),
            ("10.557", {"10"}),
            ("15", set()),
        ],
    )
    def test_ancestors(self, code, expected):
        assert hierarchy_parsers.get_cfda_ancestors(code) == expected


class TestCfdaParent:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("15.619", "15"),
            ("10.557", "10"),
            ("15", None),
        ],
    )
    def test_parent(self, code, expected):
        assert hierarchy_parsers.get_cfda_parent(code) == expected


class TestTasAncestors:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("302-2017/2018-1700-000", {"302", "302-1700"}),
            ("009-X-0200-000", {"009", "009-0200"}),
            ("019-011-X-1071-000", {"011", "011-1071"}),  # ATA present
            ("302-1700", {"302"}),
            ("302", set()),
        ],
    )
    def test_ancestors(self, code, expected):
        assert hierarchy_parsers.get_tas_ancestors(code) == expected


class TestTasParent:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ("302-2017/2018-1700-000", "302-1700"),
            ("009-X-0200-000", "009-0200"),
            ("019-011-X-1071-000", "011-1071"),  # ATA present
            ("302-1700", "302"),
            ("302", None),
        ],
    )
    def test_parent(self, code, expected):
        assert hierarchy_parsers.get_tas_parent(code) == expected
