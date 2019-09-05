"""
For Award V2 Contract/Assistance Download, the filters are quite trivial and completely unrelated to
other types of filters so they get their own file.
"""
import logging

from usaspending_api.accounts.v2.filters.account_download import generate_treasury_account_query
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator.tinyshield import TinyShield
from copy import deepcopy

logger = logging.getLogger("console")
TINYSHIELD_MODELS = [
    {
        "name": "award_id",
        "key": "award_id",
        "type": "any",
        "optional": False,
        "models": [{"type": "integer"}, {"type": "text", "text_type": "search"}],
    }
]


def _get_award_id(filters):
    TinyShield(deepcopy(TINYSHIELD_MODELS)).block(filters)
    award_id = filters["award_id"]
    return award_id


def awards_transaction_filter(filters):
    award_id = _get_award_id(filters)
    queryset = TransactionNormalized.objects.filter(award_id=award_id)
    return queryset


def awards_treasury_account_funding_filter(account_type, download_table, filters, account_level):
    if account_level != "treasury_account":
        raise InvalidParameterException("Only treasury level account reporting is supported at this time")
    award_id = _get_award_id(filters)
    queryset = download_table.objects.filter(award_id=award_id)
    queryset = generate_treasury_account_query(queryset, "award_financial", account_level)
    return queryset


def awards_subaward_filter(filters):
    award_id = _get_award_id(filters)
    return SubawardView.objects.filter(award_id=award_id)
