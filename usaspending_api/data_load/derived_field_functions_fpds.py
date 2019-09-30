from datetime import datetime, timezone

from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.data_load.cached_reference_data import subtier_agency_list
from usaspending_api.broker.helpers.get_business_categories import get_business_categories


def calculate_fiscal_year(broker_input):
    return fy(broker_input["action_date"])


def calculate_awarding_agency(broker_input):
    return _fetch_subtier_agency_id(code=broker_input["awarding_sub_tier_agency_c"])


def calculate_funding_agency(broker_input):
    return _fetch_subtier_agency_id(code=broker_input["funding_sub_tier_agency_co"])


def _fetch_subtier_agency_id(code):
    return subtier_agency_list().get(code, {}).get("id")


def unique_transaction_id(broker_input):
    return broker_input["detached_award_proc_unique"]


def current_datetime(broker_input):
    return datetime.now(timezone.utc)


def business_categories(broker_input):
    return get_business_categories(broker_input, "fpds")
