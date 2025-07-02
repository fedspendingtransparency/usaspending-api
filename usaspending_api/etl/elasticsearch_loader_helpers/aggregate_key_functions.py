import datetime
import json
import logging
from typing import Optional

from dateutil.relativedelta import relativedelta

from usaspending_api.recipient.models import RecipientProfile

logger = logging.getLogger("script")


def award_recipient_agg_key(record: dict) -> str:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["recipient_hash"] is None:
        return ""
    return str(record["recipient_hash"]) + "/" + str(record["recipient_levels"])


def transaction_recipient_agg_key(record: dict) -> str:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["recipient_hash"] is None:
        return ""
    return (
        str(record["recipient_hash"])
        + "/"
        + (RecipientProfile.return_one_level(record["recipient_levels"] or []) or "")
    )


def subaward_recipient_agg_key(record: dict) -> str:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["subaward_recipient_hash"] is None:
        return ""

    return (
        str(record["subaward_recipient_hash"])
        + "/"
        + (str(record["subaward_recipient_level"]) if record["subaward_recipient_level"] is not None else "")
    )


def awarding_subtier_agency_agg_key(record: dict) -> Optional[str]:
    return _agency_agg_key("awarding", "subtier", record)


def awarding_toptier_agency_agg_key(record: dict) -> Optional[str]:
    return _agency_agg_key("awarding", "toptier", record)


def funding_subtier_agency_agg_key(record: dict) -> Optional[str]:
    return _agency_agg_key("funding", "subtier", record)


def funding_toptier_agency_agg_key(record: dict) -> Optional[str]:
    return _agency_agg_key("funding", "toptier", record)


def _agency_agg_key(agency_type, agency_tier, record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record[f"{agency_type}_{agency_tier}_agency_name"] is None:
        return None
    result = {"name": record[f"{agency_type}_{agency_tier}_agency_name"]}
    if f"{agency_type}_{agency_tier}_agency_abbreviation" in record:
        result["abbreviation"] = record[f"{agency_type}_{agency_tier}_agency_abbreviation"]
    if f"{agency_type}_{agency_tier}_agency_code" in record:
        result["code"] = record[f"{agency_type}_{agency_tier}_agency_code"]
    result["id"] = record[f"{agency_type}_toptier_agency_id"]
    return json.dumps(result)


def naics_agg_key(record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["naics_code"] is None:
        return None
    return json.dumps({"code": record["naics_code"], "description": record["naics_description"]})


def psc_agg_key(record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["product_or_service_code"] is None:
        return None
    return json.dumps(
        {"code": record["product_or_service_code"], "description": record["product_or_service_description"]}
    )


def pop_county_agg_key(record: dict) -> Optional[str]:
    return _county_agg_key("pop", record)


def sub_pop_county_agg_key(record: dict) -> Optional[str]:
    return _county_agg_key("pop", record, True)


def recipient_location_county_agg_key(record: dict) -> Optional[str]:
    return _county_agg_key("recipient_location", record)


def sub_recipient_location_county_agg_key(record: dict) -> Optional[str]:
    return _county_agg_key("recipient_location", record, True)


def _county_agg_key(location_type, record: dict, is_subaward: bool = False) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    subaward_str = "sub_" if is_subaward else ""
    state_code_field = f"{subaward_str}{location_type}_state_code"
    county_code_field = f"{subaward_str}{location_type}_county_code"
    if record[state_code_field] is None or record[county_code_field] is None:
        return None
    return f"{record[state_code_field]}{record[county_code_field]}"


def pop_congressional_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("pop", False, record)


def pop_congressional_cur_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("pop", True, record)


def sub_pop_congressional_cur_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("pop", True, record, True)


def recipient_location_congressional_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("recipient_location", False, record)


def recipient_location_congressional_cur_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("recipient_location", True, record)


def sub_recipient_location_congressional_cur_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("recipient_location", True, record, True)


def _congressional_agg_key(location_type, current, record: dict, is_subaward: bool = False) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    cur_str = "_current" if current else ""
    subaward_str = "sub_" if is_subaward else ""
    state_code_field = f"{subaward_str}{location_type}_state_code"
    congressional_code_field = f"{subaward_str}{location_type}_congressional_code{cur_str}"
    if record[state_code_field] is None or record[congressional_code_field] is None:
        return None
    return f"{record[state_code_field]}{record[congressional_code_field]}"


def pop_state_agg_key(record: dict) -> Optional[str]:
    return _state_agg_key("pop", record)


def recipient_location_state_agg_key(record: dict) -> Optional[str]:
    return _state_agg_key("recipient_location", record)


def _state_agg_key(location_type, record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record[f"{location_type}_state_code"] is None:
        return None
    return json.dumps(
        {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_name": record[f"{location_type}_state_name"],
            "population": record[f"{location_type}_state_population"],
        }
    )


def pop_country_agg_key(record: dict) -> Optional[str]:
    return _country_agg_key("pop", record)


def recipient_location_country_agg_key(record: dict) -> Optional[str]:
    return _country_agg_key("recipient_location", record)


def _country_agg_key(location_type, record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record[f"{location_type}_country_code"] is None:
        return None
    return json.dumps(
        {
            "country_code": record[f"{location_type}_country_code"],
            "country_name": record[f"{location_type}_country_name"],
        }
    )


def location_type_agg_key(record: dict) -> Optional[str]:
    if record.get("location_json") is None:
        return ""
    else:
        json_data = record.get("location_json")

    if isinstance(json_data, str):
        return json.loads(json_data).get("location_type")
    elif isinstance(json_data, dict):
        return json_data.get("location_type")
    else:
        raise ValueError("Unable to get the 'location_type' key from the 'location_json' field")


def fiscal_action_date(record: dict) -> Optional[datetime.date]:
    if record.get("action_date") is None:
        return None
    return record["action_date"] + relativedelta(months=3)


def sub_fiscal_action_date(record: dict) -> Optional[datetime.date]:
    if record.get("sub_action_date") is None:
        return None
    return record["sub_action_date"] + relativedelta(months=3)
