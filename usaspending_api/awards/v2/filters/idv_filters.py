"""
For MVP IDV Download, the filters are quite trivial and completely unrelated to
other types of filters so they get their own file.
"""
import logging

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.awards.v2.filters.filter_helpers import get_descendant_award_ids
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.accounts.v2.filters.account_download import generate_treasury_account_query


logger = logging.getLogger(__name__)


def _get_idv_award_id(filters):
    if 'idv_award_id' not in filters:
        raise InvalidParameterException('Invalid filter: idv_award_id is required.')
    idv_award_id = filters['idv_award_id']
    if idv_award_id is None:
        raise InvalidParameterException('Invalid filter: idv_award_id has null as its value.')
    return idv_award_id


def idv_order_filter(filters):
    idv_award_id = _get_idv_award_id(filters)
    descendant_award_ids = get_descendant_award_ids(idv_award_id, True)
    if len(descendant_award_ids) < 1:
        raise InvalidParameterException('Provided IDV award id has no descendant orders')
    return Award.objects.filter(id__in=descendant_award_ids)


def idv_transaction_filter(filters):
    idv_award_id = _get_idv_award_id(filters)
    return TransactionNormalized.objects.filter(award_id=idv_award_id)


def idv_treasury_account_funding_filter(account_type, download_table, filters, account_level):
    if account_level != "treasury_account":
        raise InvalidParameterException("Only treasury level account reporting is supported at this time")
    idv_award_id = _get_idv_award_id(filters)
    descendant_award_ids = get_descendant_award_ids(idv_award_id, False)
    if len(descendant_award_ids) < 1:
        raise InvalidParameterException('Provided IDV award id has no descendants')
    queryset = download_table.objects.filter(award_id__in=descendant_award_ids)
    queryset = generate_treasury_account_query(queryset, 'award_financial', account_level)
    return queryset
