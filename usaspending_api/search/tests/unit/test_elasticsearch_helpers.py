from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize, es_sanitize


def test_es_sanitize():
    test_string = '+|()[]{}?"<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ""
    test_string = "!-^~/&:*"
    processed_string = es_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/\&\:\*"


def test_es_minimal_sanitize():
    test_string = "https://www.localhost:8000/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"https\:\/\/www.localhost\:8000\/"
    test_string = "!-^~/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/"


def test_es_minimal_sanitize_length_equality_bypass():
    """Test that the length-equality bypass vulnerability is fixed.

    Previously, when the number of removed characters equaled the number of
    escaped characters, the original unsanitized input was returned.
    This test verifies that sanitization always occurs regardless of length equality.
    """
    # Test the exact exploit payload from the vulnerability report.
    # "[* TO *]!!" has 10 chars, removes 2 brackets (8 chars), escapes 2 chars (10 chars).
    # This previously bypassed sanitization due to length equality.
    exploit_payload = "[* TO *]!!"
    processed = es_minimal_sanitize(exploit_payload)
    # Should remove brackets and escape special chars.
    assert "[" not in processed, "Brackets should be removed"
    assert "]" not in processed, "Brackets should be removed"
    assert r"\!" in processed, "Exclamation marks should be escaped"
    assert processed != exploit_payload, "Payload must be sanitized, not returned as-is"

    # Test other balanced payloads that could bypass length check.
    test_cases = [
        # (input, should_not_contain, should_contain).
        ("{test}!!", ["{", "}"], [r"\!"]),
        ("[range]:", ["[", "]"], [r"\:"]),
        ("\\test", ["\\"], []),  # Backslash removed, no escaping needed for 'test'.
        ("{[test]}!&", ["{", "}", "[", "]"], [r"\!", r"\&"]),
    ]

    for test_input, forbidden_chars, required_escapes in test_cases:
        result = es_minimal_sanitize(test_input)
        for forbidden in forbidden_chars:
            assert forbidden not in result, f"Character '{forbidden}' should be removed from '{test_input}'"
        for required in required_escapes:
            assert required in result, f"Escape sequence '{required}' should be present in result of '{test_input}'"
        assert result != test_input, f"Input '{test_input}' must be sanitized, not returned unchanged"


def test_es_minimal_sanitize_removes_lucene_operators():
    """Test that dangerous Lucene query operators are properly sanitized."""
    dangerous_inputs = [
        "[* TO *]",         # Range query.
        "{a TO z}",         # Range query with braces.
        "test\\",           # Escape character.
        "field:[value]",    # Field query with brackets.
        "test~0.5",         # Fuzzy search (~ should be escaped).
        "test^2",           # Boost operator (^ should be escaped).
    ]

    for dangerous_input in dangerous_inputs:
        result = es_minimal_sanitize(dangerous_input)
        # Verify brackets and backslashes are removed.
        assert "[" not in result
        assert "]" not in result
        assert "{" not in result
        assert "}" not in result
        assert "\\" not in result or result.count("\\") > dangerous_input.count("\\")
        # Result should differ from input (sanitized).
        assert result != dangerous_input
