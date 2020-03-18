import logging

from usaspending_api.common.helpers.timing_helpers import timer
from usaspending_api.broker.helpers.delete_stale_fabs import delete_stale_fabs


logger = logging.getLogger("console")


def delete_fabs_transactions(ids_to_delete):
    """
    ids_to_delete are afa_generated_unique ids
    """
    if ids_to_delete:
        with timer(f"deleting {len(ids_to_delete)} stale FABS data", logger.info):
            update_award_ids = delete_stale_fabs(ids_to_delete)

    else:
        update_award_ids = []
        logger.info("Nothing to delete...")

    return update_award_ids
