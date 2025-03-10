"""
For MVP IDV Download, the filters are quite trivial and completely unrelated to
other types of filters so they get their own file.
"""

import logging

from usaspending_api.accounts.v2.filters.account_download import generate_treasury_account_query
from usaspending_api.awards.v2.filters.filter_helpers import (
    get_all_award_ids_in_idv_hierarchy,
    get_descendant_award_ids,
)
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.models import AwardSearch, TransactionSearch

logger = logging.getLogger(__name__)


def _get_idv_award_id(filters):
    if "idv_award_id" not in filters:
        raise InvalidParameterException("Invalid filter: idv_award_id is required.")
    idv_award_id = filters["idv_award_id"]
    if idv_award_id is None:
        raise InvalidParameterException("Invalid filter: idv_award_id has null as its value.")
    return idv_award_id


def idv_order_filter(filters):
    idv_award_id = _get_idv_award_id(filters)
    descendant_award_ids = get_descendant_award_ids(idv_award_id, True)
    if len(descendant_award_ids) < 1:
        # If there are no descendant awards, we need a condition that cannot
        # possibly be true since we cannot pass an empty list into an __in
        # statement (well, we can but it'll throw an exception later).
        return AwardSearch.objects.filter(award_id__isnull=True)
    return AwardSearch.objects.filter(award_id__in=descendant_award_ids)


def idv_transaction_filter(filters):
    idv_award_id = _get_idv_award_id(filters)
    return TransactionSearch.objects.filter(award_id=idv_award_id)


def idv_treasury_account_funding_filter(account_type, download_table, filters, account_level):
    if account_level != "treasury_account":
        raise InvalidParameterException("Only treasury level account reporting is supported at this time")
    idv_award_id = _get_idv_award_id(filters)
    award_ids = get_all_award_ids_in_idv_hierarchy(idv_award_id)
    if len(award_ids) < 1:
        # If there are no descendant awards, we need a condition that cannot
        # possibly be true since we cannot pass an empty list into an __in
        # statement (well, we can but it'll throw an exception later).
        queryset = download_table.objects.filter(financial_accounts_by_awards_id__isnull=True)
    else:
        queryset = download_table.objects.filter(award_id__in=award_ids)
    queryset = generate_treasury_account_query(queryset, "award_financial")
    return queryset
