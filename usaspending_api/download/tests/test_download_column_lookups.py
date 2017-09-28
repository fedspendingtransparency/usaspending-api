"""
Tests setup of download_column_lookups

Don't laugh.  It's pretty simple, but there are a few things that could go wrong.
"""

import pytest
from rest_framework import status

from usaspending_api.download.v2 import download_column_historical_lookups


def test_query_paths_setup():
    for dl_type in ('transaction', 'award'):
        for file_source in ('d1', 'd2'):
            assert len(download_column_lookups.query_paths[dl_type][
                file_source]) > 10


def _values_starting_with(lst, seekme):
    return [itm for itm in lst if itm.startswith(seekme)]


def test_changes_in_award_path():
    """Verify that award and transaction query paths are different when appropriate"""

    for file_source in ('d1', 'd2'):
        assert len(
            _values_starting_with(download_column_lookups.query_paths[
                'transaction'][file_source].values(), 'transaction__')) > 10
        assert len(
            _values_starting_with(download_column_lookups.query_paths['award'][
                file_source].values(), 'transaction__')) == 0
