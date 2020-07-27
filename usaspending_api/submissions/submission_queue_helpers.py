import logging
import psycopg2

from datetime import timedelta
from django.db import connection
from django.db.models import Q
from threading import Timer
from traceback import format_exception
from typing import List, Optional, Tuple
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.submissions.models import SubmissionQueue


logger = logging.getLogger("script")


# A lock will be considered abandoned once the heartbeat becomes this old.
ABANDONED_LOCK_MINUTES = 2 * 60  # 2 hours obvs.

# This should probably be smaller than ABANDONED_LOCK_MINUTES, but don't get carried away; it does
# add a little overhead.
REFRESH_HEARTBEAT_MINUTES = 10


def get_abandoned_heartbeat_cutoff():
    return now() - timedelta(minutes=ABANDONED_LOCK_MINUTES)


def add_submission_ids(submission_ids: List[int]) -> int:
    """
    Forgivingly adds a list of submission ids to the submission queue.  Submission ids that already
    exist in the queue will remain untouched.  Returns the count of submission ids successfully added.
    """
    if not submission_ids:
        return 0

    values = ", ".join(f"({i}, '{SubmissionQueue.NEW}', false)" for i in submission_ids)

    sql = f"""
        insert into {SubmissionQueue._meta.db_table} (submission_id, state, force_reload) (
            values {values}
        ) on conflict (submission_id) do nothing
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.rowcount


def mark_force_reload(submission_ids: List[int]) -> int:
    """ Mark submissions as requiring a full reload.  Only NEW submissions can be marked. """
    if not submission_ids:
        return 0

    sql = f"""
        update {SubmissionQueue._meta.db_table} set
            force_reload = True
        where
            submission_id in %s
            and state = '{SubmissionQueue.NEW}'
            and force_reload is not True
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [tuple(submission_ids)])
        return cursor.rowcount


def claim_next_available_submission(processor_id: str) -> Tuple[Optional[int], Optional[bool]]:
    """
    Finds a submission id that requires processing, claims it, and returns the submission id.
    Returns None if there are no available submission ids remaining in the queue.
    """
    submissions = (
        SubmissionQueue.objects.filter(state=SubmissionQueue.NEW)
        .order_by("submission_id")
        .values("submission_id", "force_reload")[:1000]
    )
    for submission in submissions:
        submission_id = submission["submission_id"]
        if start_processing(submission_id, processor_id):
            return submission_id, submission["force_reload"]
    return None, None


def start_processing(submission_id: int, processor_id: str) -> int:
    """
    Claim the submission and update the processing_started timestamp.  Returns 1 if the submission
    was successfully claimed or 0 if not.  Submissions can only be claimed if they are not already
    claimed by another processor.
    """
    now_ = now()
    return SubmissionQueue.objects.filter(
        submission_id=submission_id, processor_id__isnull=True, state=SubmissionQueue.NEW
    ).update(
        state=SubmissionQueue.IN_PROGRESS,
        processor_id=processor_id,
        processing_started=now_,
        heartbeat=now_,
        processing_failed=None,
        exception=None,
    )


def update_heartbeat(submission_id: int, processor_id: str) -> int:
    """
    We maintain a heartbeat on in-progress submissions so processing can be restarted in the event
    of a silent failure.  Returns the count of updated heartbeats.  Should always return 1.  If it
    doesn't then your submission no longer exists in the queue or someone else has claimed it and
    that's probably a problem.  This uses psycopg2 instead of Django because we need a connection
    outside of those managed by Django to ensure the heartbeat is outside of any outstanding
    transactions.
    """
    sql = f"""
        update  {SubmissionQueue._meta.db_table}
        set     heartbeat = %s::timestamptz
        where   submission_id = %s and processor_id = %s and state = %s
    """
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, [now(), submission_id, processor_id, SubmissionQueue.IN_PROGRESS])
            return cursor.rowcount


def complete_processing(submission_id: int, processor_id: str) -> int:
    """
    This is a virtual state.  Completed submissions are deleted.  I like to keep a tidy queue.
    Returns the count of submissions "completed".  Should always return 1.  If it doesn't then your
    submission no longer exists in the queue or someone else has claimed it and that's probably a
    problem.
    """
    return SubmissionQueue.objects.filter(
        submission_id=submission_id, processor_id=processor_id, state=SubmissionQueue.IN_PROGRESS
    ).delete()


def fail_processing(submission_id: int, processor_id: str, exception: BaseException) -> int:
    """
    Release our claim on the submission, update the processing_failed timestamp, and record the
    exception.  Returns 1 if the submission was successfully released or 0 if not.  A submission
    that was not successfully released was likely claimed by another process either in error or
    because the heartbeat was not properly refreshed.
    """
    exception_message = "".join(format_exception(type(exception), exception, exception.__traceback__))
    return SubmissionQueue.objects.filter(
        submission_id=submission_id, processor_id=processor_id, state=SubmissionQueue.IN_PROGRESS
    ).update(state=SubmissionQueue.FAILED, processor_id=None, processing_failed=now(), exception=exception_message)


def reset_abandoned_locks() -> int:
    """
    IN_PROGRESS submissions with heartbeats older than ABANDONED_LOCK_MINUTES are considered abandoned
    and must be reset so processing can be restarted on them.  Returns the count of reset locks.
    """
    return _reset_submissions(state=SubmissionQueue.IN_PROGRESS, heartbeat__lt=get_abandoned_heartbeat_cutoff())


def reset_failed_submissions(submission_ids: List[int]) -> int:
    """
    FAILED submissions must be reset before they can be reprocessed.  Returns the count of
    submissions reset.
    """
    return _reset_submissions(state=SubmissionQueue.FAILED, submission_id__in=submission_ids)


def _reset_submissions(**filters) -> int:
    return SubmissionQueue.objects.filter(**filters).update(
        state=SubmissionQueue.NEW,
        processor_id=None,
        processing_started=None,
        heartbeat=None,
        processing_failed=None,
        exception=None,
    )


def get_queue_status() -> Tuple[List[int], List[int], List[int], List[int], List[int]]:
    """
    Looks up queue statistics and returns them in a tuple of lists containing submission_ids:

        new           - list of NEW submission ids that have not yet started processing
        in_progress   - list of submission ids currently IN_PROGRESS that are not abandoned
        abandoned     - list of IN_PROGRESS submission ids that have become abandoned
        failed        - list of submission ids that have FAILED
        unrecognized  - list of submission ids in an unrecognized state

    """
    abandoned_heartbeat_cutoff = get_abandoned_heartbeat_cutoff()

    def get_queryset(*filters, **kwfilters):
        return (
            SubmissionQueue.objects.filter(*filters, **kwfilters)
            .order_by("-submission_id")
            .values_list("submission_id", flat=True)
        )

    return (
        get_queryset(state=SubmissionQueue.NEW),
        get_queryset(state=SubmissionQueue.IN_PROGRESS, heartbeat__gte=abandoned_heartbeat_cutoff),
        get_queryset(state=SubmissionQueue.IN_PROGRESS, heartbeat__lt=abandoned_heartbeat_cutoff),
        get_queryset(state=SubmissionQueue.FAILED),
        get_queryset(Q(state__isnull=True) | ~Q(state__in=tuple(s[0] for s in SubmissionQueue.STATES))),
    )


class HeartbeatTimer(Timer):
    """
    Based on a threaded Timer, spins up a "heartbeat" timer in the background that updates the
    heartbeat value of the indicated submission in the submission queue every
    REFRESH_HEARTBEAT_MINUTES.  Dies if timer.cancel() is called or submission_id no longer
    exists in the queue.
    """

    def __init__(self, submission_id, processor_id):
        super().__init__(
            REFRESH_HEARTBEAT_MINUTES * 60,
            update_heartbeat,
            kwargs={"submission_id": submission_id, "processor_id": processor_id},
        )

    def run(self):
        self.finished.wait(self.interval)
        while not self.finished.is_set():
            logger.info(f"Updating heartbeat.")
            count = self.function(*self.args, **self.kwargs)
            if count != 1:
                logger.info(f"No heartbeat updated.  Processing has completed.")
                break
            logger.info(f"Waiting for next heartbeat.")
            self.finished.wait(self.interval)
        logger.info(f"Heartbeat timer ending.")
