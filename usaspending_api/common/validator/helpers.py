import datetime
import logging
import sys
import urllib

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.exceptions import UnprocessableEntityException

logger = logging.getLogger("console")

MAX_INT = sys.maxsize  # == 2^(63-1) == 9223372036854775807
MIN_INT = -sys.maxsize - 1  # == -2^(63-1) - 1 == 9223372036854775808
MAX_FLOAT = sys.float_info.max  # 1.7976931348623157e+308
MIN_FLOAT = -sys.float_info.max  # -1.7976931348623157e+308
MAX_ITEMS = 5000

TINY_SHIELD_SEPARATOR = "|"

INVALID_TYPE_MSG = "Invalid value in '{key}'. '{value}' is not a valid type ({type})"
ABOVE_MAXIMUM_MSG = "Field '{key}' value '{value}' is above max '{max}'"
BELOW_MINIMUM_MSG = "Field '{key}' value '{value}' is below min '{min}'"

SUPPORTED_TEXT_TYPES = ["search", "raw", "sql", "url", "password"]


def _check_max(rule):
    value = rule["value"]
    if rule["type"] in ("integer", "float"):
        if value > rule["max"]:
            raise UnprocessableEntityException(ABOVE_MAXIMUM_MSG.format(**rule))

    if rule["type"] in ("text", "enum", "array", "object"):
        if len(value) > rule["max"]:
            raise UnprocessableEntityException(ABOVE_MAXIMUM_MSG.format(**rule) + " items")


def _check_min(rule):
    value = rule["value"]
    if rule["type"] in ("integer", "float"):
        if value < rule["min"]:
            raise UnprocessableEntityException(BELOW_MINIMUM_MSG.format(**rule))

    if rule["type"] in ("text", "enum", "array", "object"):
        if len(value) < rule["min"]:
            raise UnprocessableEntityException(BELOW_MINIMUM_MSG.format(**rule) + " items")


def _check_datetime_min_max(rule, value, dt_format):

    # DEV-4097 introduces new behavior whereby minimum/maximum date violations should raise an error.  To
    # implement this, we added a min/max_exception property to the rule.  If that property exists, we will
    # raise an exception, otherwise we will retain the old behavior for backward compatibility.
    if "min" in rule:
        min_cap = datetime.datetime.strptime(rule["min"], dt_format)
        if value < min_cap:
            if "min_exception" in rule:
                message = rule["min_exception"].format(**rule)
                raise UnprocessableEntityException(message)
            logger.info("{}. Setting to {}".format(BELOW_MINIMUM_MSG.format(**rule), min_cap))
            value = min_cap

    if "max" in rule:
        max_cap = datetime.datetime.strptime(rule["max"], dt_format)
        if value > max_cap:
            if "max_exception" in rule:
                message = rule["max_exception"].format(**rule)
                raise UnprocessableEntityException(message)
            logger.info(ABOVE_MAXIMUM_MSG.format(**rule))
            value = max_cap


def _verify_int_value(value):
    if type(value) in (int, str):
        try:
            return int(value)
        except Exception:
            pass
    return None


def _verify_float_value(value):
    try:
        return float(value)
    except Exception:
        pass
    return None


def validate_array(rule):
    rule["min"] = rule.get("min") or 1
    rule["max"] = rule.get("max") or MAX_ITEMS
    value = rule["value"]
    if type(value) is not list:
        raise InvalidParameterException(INVALID_TYPE_MSG.format(**rule))
    _check_max(rule)
    _check_min(rule)
    return value


def validate_boolean(rule):
    # Could restrict this to ONLY True or False. Some tools like to use 0/1 or t/f so this function is inclusive
    if str(rule["value"]).lower() in ("1", "t", "true"):
        return True
    elif str(rule["value"]).lower() in ("0", "f", "false"):
        return False
    else:
        msg = INVALID_TYPE_MSG.format(**rule) + ". Use true/false"
        raise InvalidParameterException(msg)


def validate_datetime(rule):
    # Utilizing the Python datetime strptime since format errors are already provided by datetime methods
    dt_format = "%Y-%m-%dT%H:%M:%S"
    val = str(rule["value"])

    if len(val) == 10 or rule["type"] == "date":
        dt_format = "%Y-%m-%d"
    elif len(val) > 19:
        dt_format = "%Y-%m-%dT%H:%M:%SZ"
    try:
        value = datetime.datetime.strptime(val, dt_format)
    except ValueError:
        error_message = INVALID_TYPE_MSG.format(**rule) + ". Expected format: ({})".format(dt_format)
        raise InvalidParameterException(error_message)

    _check_datetime_min_max(rule, value, dt_format)

    # Future TODO: change this to returning the appropriate object (Date or Datetime) instead of converting to string
    if rule["type"] == "date":
        return value.date().isoformat()
    return value.isoformat() + "Z"  # adding in "zulu" timezone to keep the datetime UTC. Can switch to "+0000"


def validate_enum(rule):
    value = rule["value"]
    if value not in rule["enum_values"]:
        error_message = "Field '{}' is outside valid values {}".format(rule["key"], list(rule["enum_values"]))
        raise InvalidParameterException(error_message)
    return value


def validate_float(rule):
    rule["min"] = rule.get("min") or MIN_FLOAT
    rule["max"] = rule.get("max") or MAX_FLOAT
    temp = _verify_float_value(rule["value"])
    if temp is None:
        raise InvalidParameterException(INVALID_TYPE_MSG.format(**rule))
    rule["value"] = temp
    _check_max(rule)
    _check_min(rule)
    return rule["value"]


def validate_integer(rule):
    rule["min"] = rule.get("min") or MIN_INT
    rule["max"] = rule.get("max") or MAX_INT
    temp = _verify_int_value(rule["value"])
    if temp is None:
        raise InvalidParameterException(INVALID_TYPE_MSG.format(**rule))
    rule["value"] = temp
    _check_max(rule)
    _check_min(rule)
    return rule["value"]


def validate_object(rule):
    provided_object = rule["value"]

    if type(provided_object) is not dict:
        raise InvalidParameterException(INVALID_TYPE_MSG.format(**rule))

    if not provided_object:
        raise InvalidParameterException("'{}' is empty. Please populate object".format(rule["key"]))

    for field in provided_object.keys():
        if field not in rule["object_keys"].keys():
            raise InvalidParameterException("Unexpected field '{}' in parameter {}".format(field, rule["key"]))

    for key, value in rule["object_keys"].items():
        if key not in provided_object:
            if "optional" in value and value["optional"] is False:
                raise UnprocessableEntityException(f"Required object fields: {list(rule['object_keys'].keys())}")
            else:
                continue

    return provided_object


def validate_text(rule):
    rule["min"] = rule.get("min") or 1
    rule["max"] = rule.get("max") or MAX_ITEMS
    if type(rule["value"]) is not str:
        raise InvalidParameterException(INVALID_TYPE_MSG.format(**rule))
    _check_max(rule)
    _check_min(rule)
    search_remap = {
        ord("\t"): None,  # horizontal tab
        ord("\v"): None,  # vertical tab
        ord("\b"): None,  # ascii backspace
        ord("\f"): None,  # ascii formfeed
        ord("\r"): None,  # carriage return
        ord("\n"): None,  # newline
    }
    text_type = rule["text_type"]
    if text_type not in SUPPORTED_TEXT_TYPES:
        msg = "Invalid model {key}: '{text_type}' is not a valid text_type".format(**rule)
        raise Exception(msg + " Possible types: {}".format(SUPPORTED_TEXT_TYPES))
    if text_type in ("raw", "sql", "password"):
        # TODO: flesh out expectations and constraints for sql and password types
        if text_type in ("sql", "password"):
            logger.warning("Caution: text_type '{}' not yet fully implemented".format(text_type))
        val = rule["value"]
    elif text_type == "url":
        val = urllib.parse.quote_plus(rule["value"])
    elif text_type == "search":
        # This removes:
        #    leading and trailing whitespace
        #    ASCII escape characters
        val = rule["value"].translate(search_remap).strip()
        if val != rule["value"]:
            logger.warning(
                "Field {} value was changed from {} to {}".format(rule["key"], repr(rule["value"]), repr(val))
            )
    return val
