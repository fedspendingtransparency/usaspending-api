import logging

from usaspending_api.broker import lookups
from usaspending_api.broker.models import DeltaTableLoadVersion

logger = logging.getLogger("script")


def get_last_delta_table_load_versions(key: str):
    """
    Returns the Delta Table version IDs of the table, identified by the provided key, were loaded to staging
    tables and live Postgres tables.

    Args:
        key: Lookup key that identifies the delta table

    Returns:
        last_version_to_staging (int): Last version of the Delta Table written to the staging table in Postgres
        last_version_to_live (int): Last version of the Delta Table written to the live table in Postgres
    """
    delta_table_load_version_id = lookups.LOAD_VERSION_TYPE_DICT[key]

    last_version_to_staging, last_version_to_live = (
        DeltaTableLoadVersion.objects.filter(delta_table_load_version_id=delta_table_load_version_id)
        .values_list("last_version_copied_to_staging", "last_version_copied_to_live")
        .first()
    )

    if not last_version_to_staging or not last_version_to_live:
        logger.error(f"No record of previous run for key `{key}` was found!")
        raise ValueError(f"Record with key {key} does not exist in the DeltaTableLoadVersion table")
    else:
        logger.info(f"Value for `{key}` last_version_copied_to_staging: {last_version_to_staging}")
        logger.info(f"Value for `{key}` last_version_copied_to_live   : {last_version_to_live}")

    return last_version_to_staging, last_version_to_live


def update_last_live_load_version(key, version):
    DeltaTableLoadVersion.objects.update_or_create(
        delta_table_load_version_id=lookups.LOAD_VERSION_TYPE_DICT[key],
        defaults={"last_version_copied_to_live": version},
    )


def update_last_staging_load_version(key, version):
    DeltaTableLoadVersion.objects.update_or_create(
        delta_table_load_version_id=lookups.LOAD_VERSION_TYPE_DICT[key],
        defaults={"last_version_copied_to_staging": version},
    )
