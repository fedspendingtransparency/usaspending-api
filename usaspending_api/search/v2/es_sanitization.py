import logging
import re
from typing import Any

logger = logging.getLogger("console")


def concat_if_array(data: Any) -> str:
    if isinstance(data, str):
        return data
    else:
        if isinstance(data, list):
            str_from_array = " ".join(data)
            return str_from_array
        else:
            # This should never happen if TinyShield is functioning properly
            logger.error("Keyword submitted was not a string or array")
            return ""


def es_sanitize(input_string: str) -> str:
    """Escapes reserved elasticsearch characters and removes when necessary"""
    processed_string = re.sub(r'([|{}()?\\"+\[\]<>])', "", input_string)
    processed_string = re.sub(r"[\-]", r"\-", processed_string)
    processed_string = re.sub(r"[\^]", r"\^", processed_string)
    processed_string = re.sub(r"[~]", r"\~", processed_string)
    processed_string = re.sub(r"[/]", r"\/", processed_string)
    processed_string = re.sub(r"[!]", r"\!", processed_string)
    processed_string = re.sub(r"[&]", r"\&", processed_string)
    processed_string = re.sub(r"[:]", r"\:", processed_string)
    processed_string = re.sub(r"[`]", r"\`", processed_string)
    processed_string = re.sub(r"[*]", r"\*", processed_string)
    if len(processed_string) != len(input_string):
        msg = "Stripped characters from input string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, input_string))
    return processed_string


def es_sanitize_regex(input_string: str) -> str:
    """Escapes reserved elasticsearch characters, including those with special
    meaning in a `regexp` query (e.g. `.` matches any character and `@` matches
    any string), and removes characters when necessary.

    This should only be used for filters that are turned into `regexp` queries.
    Regular (non-regexp) queries should continue to use `es_sanitize` since `.`
    and `@` have no special meaning outside of a `regexp` query.
    """
    processed_string = es_sanitize(input_string)
    processed_string = re.sub(r"[.]", r"\.", processed_string)
    processed_string = re.sub(r"[@]", r"\@", processed_string)
    return processed_string


def es_minimal_sanitize(keyword: Any) -> str:
    keyword = concat_if_array(keyword)
    """Remove Lucene special characters and escapes when needed"""
    processed_string = re.sub(r"[{}\[\]\\]", "", keyword)
    processed_string = re.sub(r"[\-]", r"\-", processed_string)
    processed_string = re.sub(r"[\^]", r"\^", processed_string)
    processed_string = re.sub(r"[~]", r"\~", processed_string)
    processed_string = re.sub(r"[/]", r"\/", processed_string)
    processed_string = re.sub(r"[!]", r"\!", processed_string)
    processed_string = re.sub(r"[&]", r"\&", processed_string)
    processed_string = re.sub(r"[:]", r"\:", processed_string)
    processed_string = re.sub(r"[`]", r"\`", processed_string)
    if len(processed_string) != len(keyword):
        msg = "Stripped characters from ES keyword search string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, keyword))
    return processed_string
