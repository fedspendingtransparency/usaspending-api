from typing import Literal


def escape_regex_chars(value: str | None, char_set: Literal["postgres", "lucene"] = "postgres") -> str | None:
    """
    Escape regex characters

    Lucene regex metacharacters per opensearch documentation:
    https://docs.opensearch.org/latest/query-dsl/regex-syntax/#reserved-characters

    Postgres regex metacharacters: : . * + ? ^ $ [ ] ( ) { } | \

    """
    if value is None or value == "":
        return value

    char_sets = {
        "lucene": [".", "?", "+", "*", "|", "{", "}", "[", "]", "(", ")", '"', "@", "&", "~", "<", ">"],
        "postgres": [".", "*", "+", "?", "^", "$", "[", "]", "(", ")", "{", "}", "|"],
    }

    # Escape backslash first
    value = value.replace("\\", "\\\\")

    # Escape other metacharacters
    for char in char_sets[char_set]:
        value = value.replace(char, f"\\{char}")

    return value
