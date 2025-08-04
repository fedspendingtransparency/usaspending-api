"""
For Award V2 Contract/Assistance Download, the filters are quite trivial and completely unrelated to
other types of filters so they get their own file.
"""

import logging

from usaspending_api.accounts.v2.filters.account_download import generate_treasury_account_query
from usaspending_api.search.models import TransactionSearch, SubawardSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model


logger = logging.getLogger("console")


def _get_award_id(filters):
    models = get_internal_or_generated_award_id_model()
    TinyShield([models]).block(filters)
    award_id = filters["award_id"]
    return award_id


def awards_transaction_filter(filters):
    award_id = _get_award_id(filters)
    queryset = TransactionSearch.objects.filter(award_id=award_id)
    return queryset


def awards_treasury_account_funding_filter(account_type, download_table, filters, account_level):
    if account_level != "treasury_account":
        raise InvalidParameterException("Only treasury level account reporting is supported at this time")
    award_id = _get_award_id(filters)
    queryset = download_table.objects.filter(award_id=award_id)
    queryset = generate_treasury_account_query(queryset, "award_financial")
    return queryset


def awards_subaward_filter(filters):
    award_id = _get_award_id(filters)
    return SubawardSearch.objects.filter(award_id=award_id)
