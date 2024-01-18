import logging

from usaspending_api.broker import lookups
from usaspending_api.broker.models import DeltaTableLoadVersion

logger = logging.getLogger("script")


def get_last_delta_table_load_versions(key):
    # TODO - Pydoc
    delta_table_load_version_id = lookups.LOAD_VERSION_TYPE_DICT[key]

    last_version_to_staging, last_version_to_live = (
        DeltaTableLoadVersion.objects.filter(delta_table_load_version_id=delta_table_load_version_id)
            .values_list("last_version_copied_to_staging", "last_version_copied_to_live")
            .first()
    )

    if not last_version_to_staging or not last_version_to_live:
        logger.error(f"No record of previous run for key `{key}` was found!")
        # TODO - Throw an error here
    else:
        logger.info(f"Value for `{key}` last_version_copied_to_staging: {last_version_to_staging}")
        logger.info(f"Value for `{key}` last_version_copied_to_live   : {last_version_to_live}")
    
    return last_version_to_staging, last_version_to_live


def update_last_live_load_version(key, version):
    DeltaTableLoadVersion.objects.update_or_create(
        name=key,
        defaults={"last_version_copied_to_live": version}
    )


def update_last_staging_load_version(key, version):
    DeltaTableLoadVersion.objects.update_or_create(
        delta_table_load_version_id=lookups.LOAD_VERSION_TYPE_DICT[key],
        defaults={"last_version_copied_to_staging": version}
    )
