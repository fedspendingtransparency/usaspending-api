from usaspending_api.search.filters.shared.utils import escape_regex_chars


class TestEscapeRegexChars:
    """Test the shared regex escaping utility"""

    # PostgreSQL tests
    def test_postgres_basic_escaping(self):
        """Test basic PostgreSQL metacharacter escaping"""
        assert escape_regex_chars("test.value", char_set="postgres") == "test\\.value"
        assert escape_regex_chars("a*b", char_set="postgres") == "a\\*b"
        assert escape_regex_chars("x+y", char_set="postgres") == "x\\+y"
        assert escape_regex_chars("a?b", char_set="postgres") == "a\\?b"

    def test_postgres_anchors(self):
        """Test PostgreSQL anchor escaping"""
        assert escape_regex_chars("^start", char_set="postgres") == "\\^start"
        assert escape_regex_chars("end$", char_set="postgres") == "end\\$"
        assert escape_regex_chars("^start$", char_set="postgres") == "\\^start\\$"

    def test_postgres_character_classes(self):
        """Test PostgreSQL character class escaping"""
        assert escape_regex_chars("[abc]", char_set="postgres") == "\\[abc\\]"
        assert escape_regex_chars("[a-z]", char_set="postgres") == "\\[a-z\\]"
        assert escape_regex_chars("[^0-9]", char_set="postgres") == "\\[\\^0-9\\]"

    def test_postgres_grouping(self):
        """Test PostgreSQL grouping and alternation"""
        assert escape_regex_chars("(test)", char_set="postgres") == "\\(test\\)"
        assert escape_regex_chars("x|y", char_set="postgres") == "x\\|y"
        assert escape_regex_chars("{2,5}", char_set="postgres") == "\\{2,5\\}"

    def test_postgres_backslash(self):
        """Test PostgreSQL backslash escaping"""
        assert escape_regex_chars("path\\to\\file", char_set="postgres") == "path\\\\to\\\\file"
        assert escape_regex_chars("\\", char_set="postgres") == "\\\\"

    def test_postgres_redos_pattern(self):
        """Test escaping of ReDoS patterns (f145 vulnerability)"""
        # The actual vulnerability pattern
        assert escape_regex_chars(".*.*.*.*", char_set="postgres") == "\\.\\*\\.\\*\\.\\*\\.\\*"
        assert escape_regex_chars("(a+)+", char_set="postgres") == "\\(a\\+\\)\\+"
        assert escape_regex_chars("(a*)*", char_set="postgres") == "\\(a\\*\\)\\*"

    # Lucene tests
    def test_lucene_basic_escaping(self):
        """Test basic Lucene metacharacter escaping"""
        assert escape_regex_chars("test.value", char_set="lucene") == "test\\.value"
        assert escape_regex_chars("a*b", char_set="lucene") == "a\\*b"
        assert escape_regex_chars("x+y", char_set="lucene") == "x\\+y"
        assert escape_regex_chars("a?b", char_set="lucene") == "a\\?b"

    def test_lucene_quotes(self):
        """Test Lucene quote escaping"""
        assert escape_regex_chars('"hello"', char_set="lucene") == '\\"hello\\"'
        assert escape_regex_chars('say "world"', char_set="lucene") == 'say \\"world\\"'

    def test_lucene_optional_operators(self):
        """Test Lucene optional operator escaping"""
        assert escape_regex_chars("@string", char_set="lucene") == "\\@string"
        assert escape_regex_chars("a&b", char_set="lucene") == "a\\&b"
        assert escape_regex_chars("~pattern", char_set="lucene") == "\\~pattern"
        assert escape_regex_chars("<10-20>", char_set="lucene") == "\\<10-20\\>"

    def test_lucene_character_classes(self):
        """Test Lucene character class escaping"""
        assert escape_regex_chars("[abc]", char_set="lucene") == "\\[abc\\]"
        assert escape_regex_chars("[a-z]", char_set="lucene") == "\\[a-z\\]"

    def test_lucene_grouping(self):
        """Test Lucene grouping and alternation"""
        assert escape_regex_chars("(test)", char_set="lucene") == "\\(test\\)"
        assert escape_regex_chars("x|y", char_set="lucene") == "x\\|y"
        assert escape_regex_chars("{2,5}", char_set="lucene") == "\\{2,5\\}"

    def test_lucene_backslash(self):
        """Test Lucene backslash escaping"""
        assert escape_regex_chars("path\\to\\file", char_set="lucene") == "path\\\\to\\\\file"

    def test_lucene_dfa_explosion_pattern(self):
        """Test escaping of DFA state explosion patterns (f070 vulnerability)"""
        assert escape_regex_chars(".*.*.*.*.*", char_set="lucene") == "\\.\\*\\.\\*\\.\\*\\.\\*\\.\\*"
        assert escape_regex_chars("a+b+c+d+", char_set="lucene") == "a\\+b\\+c\\+d\\+"

    # Edge cases
    def test_normal_characters_not_escaped(self):
        """Test that normal characters are not escaped"""
        assert escape_regex_chars("abc123", char_set="postgres") == "abc123"
        assert escape_regex_chars("test-value_123", char_set="postgres") == "test-value_123"
        assert escape_regex_chars("abc123", char_set="lucene") == "abc123"
        assert escape_regex_chars("test-value_123", char_set="lucene") == "test-value_123"

    def test_empty_string(self):
        """Test empty string handling"""
        assert escape_regex_chars("", char_set="postgres") == ""
        assert escape_regex_chars("", char_set="lucene") == ""

    def test_none_value(self):
        """Test None value handling"""
        assert escape_regex_chars(None, char_set="postgres") is None
        assert escape_regex_chars(None, char_set="lucene") is None

    def test_default_char_set(self):
        """Test that postgres is the default char_set"""
        # Should default to postgres
        assert escape_regex_chars("^test$") == "\\^test\\$"
        # Lucene-specific chars should not be escaped with default
        assert escape_regex_chars('"test"') == '"test"'

    def test_mixed_special_characters(self):
        """Test strings with multiple special characters"""
        postgres_input = "test.*+?^$[a-z](group){2,5}|alt"
        postgres_expected = "test\\.\\*\\+\\?\\^\\$\\[a-z\\]\\(group\\)\\{2,5\\}\\|alt"
        assert escape_regex_chars(postgres_input, char_set="postgres") == postgres_expected

        lucene_input = 'test.*+?[a-z](group){2,5}|"quoted"@&~<>'
        lucene_expected = 'test\\.\\*\\+\\?\\[a-z\\]\\(group\\)\\{2,5\\}\\|\\"quoted\\"\\@\\&\\~\\<\\>'
        assert escape_regex_chars(lucene_input, char_set="lucene") == lucene_expected

    def test_consecutive_backslashes(self):
        """Test multiple consecutive backslashes"""
        assert escape_regex_chars("\\\\", char_set="postgres") == "\\\\\\\\"
        assert escape_regex_chars("\\\\\\", char_set="postgres") == "\\\\\\\\\\\\"

    def test_real_world_tas_values(self):
        """Test with realistic TAS component values"""
        # Normal TAS values should pass through unchanged
        assert escape_regex_chars("015", char_set="postgres") == "015"
        assert escape_regex_chars("0324", char_set="postgres") == "0324"
        assert escape_regex_chars("000", char_set="postgres") == "000"
        assert escape_regex_chars("2017", char_set="postgres") == "2017"

        # TAS values with hyphens
        assert escape_regex_chars("012-345", char_set="postgres") == "012-345"
