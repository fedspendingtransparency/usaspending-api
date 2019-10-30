from datetime import datetime, timezone
import re

from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.etl.transaction_loaders.cached_reference_data import subtier_agency_list
from usaspending_api.broker.helpers.get_business_categories import get_business_categories
from usaspending_api.common.helpers.date_helper import cast_datetime_to_utc


def calculate_fiscal_year(broker_input):
    return fy(broker_input["action_date"])


def calculate_awarding_agency(broker_input):
    return _fetch_subtier_agency_id(code=broker_input["awarding_sub_tier_agency_c"])


def calculate_funding_agency(broker_input):
    return _fetch_subtier_agency_id(code=broker_input["funding_sub_tier_agency_co"])


def _fetch_subtier_agency_id(code):
    return subtier_agency_list().get(code, {}).get("id")


def current_datetime(broker_input):
    return datetime.now(timezone.utc)


def business_categories(broker_input):
    return get_business_categories(broker_input, "fpds")


def created_at(broker_input):
    return cast_datetime_to_utc(broker_input["created_at"])


def updated_at(broker_input):
    return cast_datetime_to_utc(broker_input["updated_at"])


def legal_entity_zip5(broker_input):
    zip_code_pattern = re.compile(r"^(\d{5})\-?(\d{4})?$")
    if broker_input["legal_entity_zip5"]:
        return broker_input["legal_entity_zip5"]
    elif broker_input["legal_entity_zip4"]:
        match = zip_code_pattern.match(broker_input["legal_entity_zip4"])
        if match:
            return match.group(1)
    return ""


def place_of_performance_zip5(broker_input):
    zip_code_pattern = re.compile(r"^(\d{5})\-?(\d{4})?$")
    if broker_input["place_of_performance_zip5"]:
        return broker_input["place_of_performance_zip5"]
    elif broker_input["place_of_performance_zip4a"]:
        match = zip_code_pattern.match(broker_input["place_of_performance_zip4a"])
        if match:
            return match.group(1)
    return ""
