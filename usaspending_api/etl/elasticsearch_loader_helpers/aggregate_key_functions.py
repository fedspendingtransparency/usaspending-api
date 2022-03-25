import json
import logging

from typing import Optional, List


logger = logging.getLogger("script")


def award_recipient_agg_key(record: dict) -> str:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["recipient_hash"] is None or record["recipient_levels"] is None:
        return json.dumps(
            {
                "name": record["recipient_name"],
                "duns": record["recipient_unique_id"],
                "uei": record["recipient_uei"],
                "hash": "",
                "levels": "",
            }
        )
    return json.dumps(
        {
            "name": record["recipient_name"],
            "duns": record["recipient_unique_id"],
            "uei": record["recipient_uei"],
            "hash": str(record["recipient_hash"]),
            "levels": record["recipient_levels"],
        }
    )


def transaction_recipient_agg_key(record: dict) -> str:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record["recipient_hash"] is None or record["recipient_levels"] is None:
        return json.dumps(
            {
                "name": record["recipient_name"],
                "duns": record["recipient_unique_id"],
                "uei": record["recipient_uei"],
                "hash_with_level": "",
            }
        )
    return json.dumps(
        {
            "name": record["recipient_name"],
            "duns": record["recipient_unique_id"],
            "uei": record["recipient_uei"],
            "hash_with_level": f"{record['recipient_hash']}-{return_one_level(record['recipient_levels'])}",
        }
    )


def return_one_level(levels: List[str]) -> Optional[str]:
    """Return the most-desirable recipient level"""
    for level in ("C", "R", "P"):  # Child, "Recipient," or Parent
        if level in levels:
            return level
    else:
        return None


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


def recipient_location_county_agg_key(record: dict) -> Optional[str]:
    return _county_agg_key("recipient_location", record)


def _county_agg_key(location_type, record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record[f"{location_type}_state_code"] is None or record[f"{location_type}_county_code"] is None:
        return None
    return json.dumps(
        {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_fips": record[f"{location_type}_state_fips"],
            "county_code": record[f"{location_type}_county_code"],
            "county_name": record[f"{location_type}_county_name"],
            "population": record[f"{location_type}_county_population"],
        }
    )


def pop_congressional_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("pop", record)


def recipient_location_congressional_agg_key(record: dict) -> Optional[str]:
    return _congressional_agg_key("recipient_location", record)


def _congressional_agg_key(location_type, record: dict) -> Optional[str]:
    """Dictionary key order impacts Elasticsearch behavior!!!"""
    if record[f"{location_type}_state_code"] is None or record[f"{location_type}_congressional_code"] is None:
        return None
    return json.dumps(
        {
            "country_code": record[f"{location_type}_country_code"],
            "state_code": record[f"{location_type}_state_code"],
            "state_fips": record[f"{location_type}_state_fips"],
            "congressional_code": record[f"{location_type}_congressional_code"],
            "population": record[f"{location_type}_congressional_population"],
        }
    )


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
