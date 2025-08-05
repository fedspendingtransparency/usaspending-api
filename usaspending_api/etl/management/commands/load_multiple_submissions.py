import logging

from datetime import timedelta
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max
from django.utils.crypto import get_random_string
from usaspending_api.common.helpers.date_helper import now, datetime_command_line_argument_type
from usaspending_api.etl.submission_loader_helpers.final_of_fy import populate_final_of_fy
from usaspending_api.etl.submission_loader_helpers.submission_ids import get_new_or_updated_submission_ids
from usaspending_api.submissions import dabs_loader_queue_helpers as dlqh
from usaspending_api.submissions.models import SubmissionAttributes


logger = logging.getLogger("script")


DISPLAY_CAP = 100


class Command(BaseCommand):
    help = (
        "The goal of this management command is coordinate the loading of multiple submissions "
        "simultaneously using the load_submission single submission loader.  To load submissions "
        "in parallel, kick off multiple runs at the same time.  Runs will be coordinated via the "
        "dabs_loader_queue table in the database which allows loaders to be run from different "
        "machines in different environments.  Using the database as the queue sidesteps the AWS "
        "SQS 24 hour message lifespan limitation.  There is no hard cap on the number of jobs that "
        "can be run simultaneously, but certainly there is a soft cap imposed by resource "
        "contention.  During development, 8 were run in parallel without incident."
    )

    submission_ids = None
    incremental = False
    start_datetime = None
    report_queue_status_only = False
    processor_id = None
    heartbeat_timer = None
    file_c_chunk_size = 100000
    do_not_retry = []

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
        mutually_exclusive_group.add_argument(
            "--submission-ids",
            help=(
                "One or more Broker submission_ids to be reloaded.  These submissions are added to "
                "the submission queue and processing begins on them immediately.  Due to the "
                "asynchronous, multiprocessing nature of the submission queue, it is possible that "
                "another loader might nab and/or complete one or more of these submissions before "
                "we get to them.  This is just the nature of the beast.  The logs will document "
                "when this happens.  Submissions loaded in this manner will be fully reloaded unless "
                "another process is currently loading the submission."
            ),
            nargs="+",
            type=int,
        )
        mutually_exclusive_group.add_argument(
            "--incremental",
            action="store_true",
            help=(
                "Loads new or updated submissions in Broker since the most recently published "
                "submission in USAspending.  Submissions loaded in this manner will be updated "
                "where possible.  Otherwise they will be fully reloaded."
            ),
        )
        mutually_exclusive_group.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help=(
                "Loads new or updated submissions in Broker since the timestamp provided.  This is "
                "effectively the same as the --incremental option except the start date/time is "
                "specified on the command line."
            ),
        )
        mutually_exclusive_group.add_argument(
            "--report-queue-status-only",
            action="store_true",
            help="Just reports the queue status.  Nothing is loaded.",
        )
        parser.add_argument(
            "--file-c-chunk-size",
            type=int,
            default=self.file_c_chunk_size,
            help=(
                f"Controls the number of File C records processed in a single batch.  Theoretically, "
                f"bigger should be faster... right up until you run out of memory.  Balance carefully.  "
                f"Default is {self.file_c_chunk_size:,}."
            ),
        )
        parser.add_argument(
            "--skip-c-to-d-linkage",
            action="store_true",
            help=(
                "This flag skips the step to perform File C to D Linkages, which updates the "
                "`award_id` field on File C records. File C to D linkages also take place in "
                "subsequent Databricks steps in the pipeline and only takes place in this "
                "command for earlier data consistency. It can safely be skipped in the case of "
                "long running submissions.",
            ),
        )

        parser.epilog = (
            "And to answer your next question, yes this can be run standalone.  The parallelization "
            "code is pretty minimal and should not add significant time to the overall run time of "
            "serial submission loads."
        )

    def handle(self, *args, **options):
        self.record_options(options)

        self.report_queue_status()
        if self.report_queue_status_only:
            return

        self.reset_abandoned_locks()

        if self.submission_ids:
            self.add_specific_submissions_to_queue()
            processed_count = self.load_specific_submissions()
        else:
            since_datetime = self.start_datetime or self.calculate_load_submissions_since_datetime()
            self.add_submissions_since_datetime_to_queue(since_datetime)
            processed_count = self.load_incremental_submissions()

        ready, in_progress, abandoned, failed, unrecognized = dlqh.get_queue_status()
        failed_unrecognized_and_abandoned_count = len(failed) + len(unrecognized) + len(abandoned)
        in_progress_count = len(in_progress)

        self.update_final_of_fy(processed_count, in_progress_count)

        # Only return unstable state if something's in a bad state and we're the last one standing.
        # Should cut down on Slack noise a bit.
        if failed_unrecognized_and_abandoned_count > 0 and in_progress_count == 0:
            raise SystemExit(3)

    def record_options(self, options):
        self.submission_ids = options.get("submission_ids")
        self.incremental = options.get("incremental")
        self.start_datetime = options.get("start_datetime")
        self.report_queue_status_only = options.get("report_queue_status_only")
        self.file_c_chunk_size = options.get("file_c_chunk_size")
        self.skip_c_to_d_linkage = options["skip_c_to_d_linkage"]
        self.processor_id = f"{now()}/{get_random_string(length=12)}"

        logger.info(f'processor_id = "{self.processor_id}"')

    @staticmethod
    def report_queue_status():
        """
        Logs various information about the state of the submission queue.  Returns a count of failed
        and unrecognized submissions so the caller can whine about it if they're so inclined.
        """
        ready, in_progress, abandoned, failed, unrecognized = dlqh.get_queue_status()
        overall_count = sum(len(s) for s in (ready, in_progress, abandoned, failed, unrecognized))

        msg = [
            "The current queue status is as follows:\n",
            f"There are {overall_count:,} total submissions in the queue.",
            f"   {len(ready):,} are ready but have not yet started processing.",
            f"   {len(in_progress):,} are in progress.",
            f"   {len(abandoned):,} have been abandoned.",
            f"   {len(failed):,} have FAILED.",
            f"   {len(unrecognized):,} are in an unrecognized state.",
        ]

        def log_submission_ids(submissions, message):
            if submissions:
                caveat = f" (first {DISPLAY_CAP:,} shown)" if len(submissions) > DISPLAY_CAP else ""
                submissions = ", ".join(str(s) for s in submissions[:DISPLAY_CAP])
                msg.extend(["", f"The following submissions {message}{caveat}: {submissions}"])

        log_submission_ids(in_progress, "are in progress")
        log_submission_ids(abandoned, "have been abandoned")
        log_submission_ids(failed, "have failed")
        log_submission_ids(unrecognized, "are in an unrecognized state")

        logger.info("\n".join(msg) + "\n")

    @staticmethod
    def reset_abandoned_locks():
        count = dlqh.reset_abandoned_locks()
        if count > 0:
            logger.info(f"Reset {count:,} abandoned locks.")
        return count

    def add_specific_submissions_to_queue(self):
        with transaction.atomic():
            added = dlqh.add_submission_ids(self.submission_ids)
            dlqh.mark_force_reload(self.submission_ids)
        count = len(self.submission_ids)
        logger.info(
            f"Received {count:,} submission ids on the command line.  {added:,} were "
            f"added to the queue.  {count - added:,} already existed."
        )

    def load_specific_submissions(self):
        processed_count = 0
        for submission_id in self.submission_ids:
            count = dlqh.start_processing(submission_id, self.processor_id)
            if count == 0:
                logger.info(f"Submission {submission_id} has already been picked up by another processor.  Skipping.")
            else:
                self.load_submission(submission_id, force_reload=True)
                processed_count += 1
        return processed_count

    @staticmethod
    def add_submissions_since_datetime_to_queue(since_datetime):
        if since_datetime is None:
            logger.info("No records found in submission_attributes.  Performing a full load.")
        else:
            logger.info(f"Performing incremental load starting from {since_datetime}.")
        submission_ids = get_new_or_updated_submission_ids(since_datetime)
        added = dlqh.add_submission_ids(submission_ids)
        count = len(submission_ids)
        logger.info(
            f"Identified {count:,} new or updated submission ids in Broker.  {added:,} were "
            f"added to the queue.  {count - added:,} already existed."
        )

    def load_incremental_submissions(self):
        processed_count = 0
        while True:
            submission_id, force_reload = dlqh.claim_next_available_submission(self.processor_id, self.do_not_retry)
            if submission_id is None:
                logger.info("No more available submissions in the queue.  Exiting.")
                break
            self.load_submission(submission_id, force_reload)
            processed_count += 1
        return processed_count

    def cancel_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

    def start_heartbeat_timer(self, submission_id):
        if self.heartbeat_timer:
            self.cancel_heartbeat_timer()
        self.heartbeat_timer = dlqh.HeartbeatTimer(submission_id, self.processor_id)
        self.heartbeat_timer.start()

    def load_submission(self, submission_id, force_reload):
        """
        Accepts a locked/claimed submission id, spins up a heartbeat thread, loads the submission,
        returns True if successful or False if not.
        """
        args = ["--file-c-chunk-size", self.file_c_chunk_size, "--skip-final-of-fy-calculation"]
        if force_reload:
            args.append("--force-reload")
        if self.skip_c_to_d_linkage:
            args.append("--skip-c-to-d-linkage")
        self.start_heartbeat_timer(submission_id)
        try:
            call_command("load_submission", submission_id, *args)
        except (Exception, SystemExit) as e:
            self.cancel_heartbeat_timer()
            logger.exception(f"Submission {submission_id} failed to load")
            dlqh.fail_processing(submission_id, self.processor_id, e)
            self.do_not_retry.append(submission_id)
            self.report_queue_status()
            return False
        self.cancel_heartbeat_timer()
        dlqh.complete_processing(submission_id, self.processor_id)
        self.report_queue_status()
        return True

    @staticmethod
    def calculate_load_submissions_since_datetime():
        since = SubmissionAttributes.objects.all().aggregate(Max("published_date"))["published_date__max"]
        if since:
            # In order to prevent skips, we're just always going to look back 30 days.  Since submission is a
            # relatively low volume table, this should not cause any noticeable performance issues.
            since -= timedelta(days=30)
        return since

    @staticmethod
    def update_final_of_fy(processed_count, in_progress_count):
        """
        For performance and deadlocking reasons, we only update final_of_fy once the last
        submission is processed.  To this end, only update final_of_fy if any loads were
        performed and there's nothing processable left in the queue.
        """
        if processed_count < 1:
            logger.info("No work performed.  Not updating final_of_fy.")
            return
        if in_progress_count > 0:
            logger.info("Submissions still in progress.  Not updating final_of_fy.")
            return
        logger.info("Updating final_of_fy")
        populate_final_of_fy()
        logger.info(f"Finished updating final_of_fy.")
