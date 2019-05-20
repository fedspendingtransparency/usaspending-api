import logging

from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger('console')


def verify_requested_columns_available(sources, requested):
    """Ensures the user-requested columns are availble to write to"""
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))
