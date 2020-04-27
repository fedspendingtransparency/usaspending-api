import logging
import re

from usaspending_api.search.v2.elasticsearch_helper import concat_if_array

logger = logging.getLogger("console")


def es_sanitize(input_string):
    """ Escapes reserved elasticsearch characters and removes when necessary """

    processed_string = re.sub(r'([-&!|{}()^~*?:\\/"+\[\]<>])', "", input_string)
    if len(processed_string) != len(input_string):
        msg = "Stripped characters from input string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, input_string))
    return processed_string


def es_minimal_sanitize(keyword):
    keyword = concat_if_array(keyword)
    """Remove Lucene special characters instead of escaping for now"""
    processed_string = re.sub(r"[%{}/:!^\[\]]", "", keyword)
    if len(processed_string) != len(keyword):
        msg = "Stripped characters from ES keyword search string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, keyword))
        keyword = processed_string
    return keyword