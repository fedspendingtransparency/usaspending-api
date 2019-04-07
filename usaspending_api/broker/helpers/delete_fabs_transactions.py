import logging

from usaspending_api.common.helpers.generic_helper import timer
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs
from usaspending_api.broker.helpers.store_deleted_fabs import store_deleted_fabs


logger = logging.getLogger("console")


def delete_fabs_transactions(ids_to_delete, do_not_log_deletions):
    """
    ids_to_delete are afa_generated_unique ids
    """
    if ids_to_delete:
        if do_not_log_deletions is False:
            store_deleted_fabs(ids_to_delete)
        with timer("deleting stale FABS data", logger.info):
            update_award_ids = delete_stale_fabs(ids_to_delete)

    else:
        update_award_ids = []
        logger.info("Nothing to delete...")

    return update_award_ids
