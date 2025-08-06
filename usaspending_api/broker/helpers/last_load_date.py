import logging

from datetime import timedelta

from usaspending_api.broker import lookups
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.common.helpers.date_helper import cast_datetime_to_utc

logger = logging.getLogger("script")


def get_last_load_date(key, lookback_minutes=None, default=None, format_func: callable = (lambda _: _)):
    """
    Retrieve the last_load_date from the USAspending database.

    Valid keys are dictated by the keys in EXTERNAL_DATA_TYPE_DICT.

    lookback_minutes is used to provide some protection against gaps caused by
    long transactions or race conditions.  It will be subtracted from
    last_load_date.  NOTE:  It will not be subtracted from the default in the
    case where no last_load_date is found.

    default will be returned if no last_load_date is found in the database.
    """
    external_data_type_id = lookups.EXTERNAL_DATA_TYPE_DICT[key]
    last_load_date = (
        ExternalDataLoadDate.objects.filter(external_data_type_id=external_data_type_id)
        .values_list("last_load_date", flat=True)
        .first()
    )
    if last_load_date is None:
        logger.warning(format_func(f"No record of a previous run for `{key}` was found!"))
        return default
    else:
        logger.info(format_func(f"Value for previous `{key}` ETL: {last_load_date}"))
    if lookback_minutes is not None:
        last_load_date -= timedelta(minutes=lookback_minutes)
    return last_load_date


def get_earliest_load_date(keys, default=None, format_func: callable = (lambda _: _)):
    """
    Retrieve the earliest last_load_date from a supplied list of keys.

    default will be returned only if no last_load_date is found for any of the supplied keys
    """
    earliest_date = None

    for key in keys:
        key_date = get_last_load_date(key, format_func=format_func)

        if key_date:
            if earliest_date is None:
                earliest_date = key_date
            elif key_date < earliest_date:
                earliest_date = key_date

    if earliest_date is None:
        logger.warning(
            format_func(f"No earliest load date could be calculated because no dates for keys `{keys}` were found!")
        )
        return default

    return earliest_date


def get_latest_load_date(keys, default=None, format_func: callable = (lambda _: _)):
    """
    Retrieve the latest last_load_date from a supplied list of keys.

    default will be returned only if no last_load_date is found for any of the supplied keys
    """
    latest_date = None

    for key in keys:
        key_date = get_last_load_date(key, format_func=format_func)

        if key_date:
            if latest_date is None:
                latest_date = key_date
            elif key_date > latest_date:
                latest_date = key_date

    if latest_date is None:
        logger.warning(
            format_func(f"No latest load date could be calculated because no dates for keys `{keys}` were found!")
        )
        return default

    return latest_date


def update_last_load_date(key, last_load_date):
    """
    Save the provided last_load_date to the database as UTC (which is our standard timezone).
    """
    ExternalDataLoadDate.objects.update_or_create(
        external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT[key],
        defaults={"last_load_date": cast_datetime_to_utc(last_load_date)},
    )
