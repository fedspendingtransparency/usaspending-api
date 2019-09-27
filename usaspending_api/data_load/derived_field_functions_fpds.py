from datetime import datetime, timezone

from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.data_load.cached_reference_data import subtier_agency_list
from usaspending_api.broker.helpers.get_business_categories import get_business_categories


def calculate_fiscal_year(broker_input):
    return fy(broker_input["action_date"])


def calculate_awarding_agency(broker_input):
    awarding_agency = subtier_agency_list().get(broker_input["awarding_sub_tier_agency_c"], None)
    if awarding_agency is not None:
        return awarding_agency["id"]
    else:
        return None


def calculate_funding_agency(broker_input):
    funding_agency = subtier_agency_list().get(broker_input["funding_sub_tier_agency_co"], None)
    if funding_agency is not None:
        return funding_agency["id"]
    else:
        return None


def unique_transaction_id(broker_input):
    return broker_input["detached_award_proc_unique"]


def current_datetime(broker_input):
    return datetime.now(timezone.utc)


def business_categories(broker_input):
    return get_business_categories(broker_input, "fpds")
