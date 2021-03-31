import logging

from datetime import datetime
from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger(__name__)


def check_types_and_assign_defaults(old_dict, new_dict, defaults_dict):
    """Validates the types of the old_dict, and assigns them to the new_dict"""
    for field in defaults_dict.keys():
        # Set the new value to the old value, or the default if it doesn't exist
        new_dict[field] = old_dict.get(field, defaults_dict[field])

        # Validate the field's data type.  psc_codes:  I hate doing this so much, but I don't know
        # a better way without redesigning how validation works which is definitely outside of the
        # scope of this ticket and adds risk.  In a nutshell, psc_codes can support lists or dicts.
        # The original function is designed to match the type of the field value with the type of
        # its default value which doesn't permit more than a single type.
        if field == "psc_codes":
            if not isinstance(new_dict[field], type(defaults_dict[field])) and not isinstance(new_dict[field], dict):
                type_name = type(defaults_dict[field]).__name__
                raise InvalidParameterException(f"{field} parameter not provided as a {type_name} or dict")
        elif not isinstance(new_dict[field], type(defaults_dict[field])):
            type_name = type(defaults_dict[field]).__name__
            raise InvalidParameterException(f"{field} parameter not provided as a {type_name}")

        # Remove empty filters
        if new_dict[field] == defaults_dict[field]:
            del new_dict[field]


def get_date_range_length(date_range):
    d1 = datetime.strptime(date_range["start_date"], "%Y-%m-%d")
    d2 = datetime.strptime(date_range["end_date"], "%Y-%m-%d")
    return (d2 - d1).days
