import logging

from collections import namedtuple
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_named_tuple
from usaspending_api.etl.management.helpers.load_submission import (
    calculate_load_submissions_since_datetime,
    get_publish_history_table,
)
from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger("script")


class Command(BaseCommand):
    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
        mutually_exclusive_group.add_argument(
            "--submission-ids",
            help=("Optionally supply one or more Broker submission_ids to be created or updated."),
            nargs="+",
            type=int,
        )
        mutually_exclusive_group.add_argument(
            "--incremental", action="store_true", help="Loads newly created or updated submissions.",
        )
        mutually_exclusive_group.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help=(
                "Manually set the date from which to start loading submissions.  This was originally designed "
                "to be used for testing, but there are definitely real world usages for it... just be careful."
            ),
        )
        parser.add_argument(
            "--list-ids-only",
            action="store_true",
            help="Only list submissions to be loaded.  Do not actually load them.",
        )

    def handle(self, *args, **options):
        since_datetime = options.get("start_datetime") or calculate_load_submissions_since_datetime()
        if since_datetime is None:
            logger.info("No records found in submission_attributes.  Performing a full load.")
        else:
            logger.info(f"Performing incremental load starting from {since_datetime}.")

        if options["submission_ids"]:
            certified_only_submission_ids, load_submission_ids = [], options["submission_ids"]
        else:
            certified_only_submission_ids, load_submission_ids = self.get_incremental_submission_ids(since_datetime)

        if certified_only_submission_ids:
            msg = f"{len(certified_only_submission_ids):,} submissions will only receive certified_date updates"
            if len(certified_only_submission_ids) <= 1000:
                logger.info(f"The following {msg}: {[c.submission_id for c in certified_only_submission_ids]}")
            else:
                logger.info(f"{msg}.")

        if load_submission_ids:
            msg = f"{len(load_submission_ids):,} submissions will be created or updated"
            if len(load_submission_ids) <= 1000:
                logger.info(f"The following {msg}: {load_submission_ids}")
            else:
                logger.info(f"{msg}.")

        if not certified_only_submission_ids and not load_submission_ids:
            logger.info("There are no new or updated submissions.")
            return

        if options["list_ids_only"]:
            logger.info("Exiting script before data load occurs in accordance with the --list-ids-only flag.")
            return

        self.process_submissions(certified_only_submission_ids, load_submission_ids)

    @staticmethod
    def process_submissions(certified_only_submission_ids, load_submission_ids):
        failed_submissions = []

        for submission in certified_only_submission_ids:
            try:
                rows_affected = SubmissionAttributes.objects.filter(submission_id=submission.submission_id).update(
                    certified_date=submission.new_certified_date
                )
                if rows_affected < 1:
                    raise RuntimeError(f"No rows affected for {submission.submission_id} when update was executed.")
            except (Exception, SystemExit):
                logger.exception(f"Submission {submission.submission_id} failed to update")
                failed_submissions.append(submission.submission_id)

        for submission_id in load_submission_ids:
            try:
                call_command("load_submission", submission_id)
            except (Exception, SystemExit):
                logger.exception(f"Submission {submission_id} failed to load")
                failed_submissions.append(submission_id)

        if failed_submissions:
            logger.error(
                f"Script completed with the following {len(failed_submissions):,} "
                f"submission failures: {failed_submissions}"
            )
            raise SystemExit(3)
        else:
            logger.info("Script completed with no failures.")

    @staticmethod
    def get_since_sql(since_datetime=None):
        """
        For performance reasons, we intentionally use updated_at here even though we're comparing against
        published_date later.  submission.updated_at should always be greater than or equal to published_date.
        """
        if since_datetime is None:
            return ""
        return f"and s.updated_at >= ''{since_datetime}''::timestamp"

    @classmethod
    def get_incremental_submission_ids(cls, since_datetime=None):
        """
        Identifies Broker submissions that need to be created or updated in USAspending.  If supplied a
        start_datetime, limits Broker rows to that date range.  If not, compares ALL Broker submissions.
        Operates in two modes simultaneously:
            MODE 1:  Detect when there are only changes in certification date.
            MODE 2:  Detect when there are changes in any field other than certification date.
        Certified-date-only changes do not require a full reload and thus provide us with a shortcut.
        Note that this is designed to work with our conservative lookback period by filtering out rows
        that haven't changed.  Look back as far as you want!

        Returns two lists:
            LIST 1:  Tuple of submission ids and new certified_date when only the certified_date has changed.
            LIST 2:  Submission ids when anything other than just the certified_date has changed.
        """
        sql = f"""
            select
                submission_id, new_certified_date, anything_other_than_certified_date_has_changed
            from (
                    select
                        bs.submission_id,
                        bs.published_date,
                        case
                            when sa.certified_date::timestamp is distinct from bs.certified_date then bs.certified_date
                        end as new_certified_date,
                        (
                            sa.published_date::timestamp is distinct from bs.published_date or
                            sa.toptier_code is distinct from bs.toptier_code or
                            sa.reporting_period_start is distinct from bs.reporting_start_date or
                            sa.reporting_period_end is distinct from bs.reporting_end_date or
                            sa.reporting_fiscal_year is distinct from bs.reporting_fiscal_year or
                            sa.reporting_fiscal_period is distinct from bs.reporting_fiscal_period or
                            sa.quarter_format_flag is distinct from bs.is_quarter_format
                        ) as anything_other_than_certified_date_has_changed
                    from
                        dblink(
                            '{settings.DATA_BROKER_DBLINK_NAME}',
                            '
                                select
                                    s.submission_id,
                                    (
                                        select  max(updated_at)
                                        from    {get_publish_history_table()}
                                        where   submission_id = s.submission_id
                                    ) as published_date,
                                    (
                                        select  max(updated_at)
                                        from    certify_history
                                        where   submission_id = s.submission_id
                                    ) as certified_date,
                                    coalesce(s.cgac_code, s.frec_code) as toptier_code,
                                    s.reporting_start_date,
                                    s.reporting_end_date,
                                    s.reporting_fiscal_year,
                                    s.reporting_fiscal_period,
                                    s.is_quarter_format
                                from
                                    submission as s
                                where
                                    s.d2_submission is false and
                                    s.publish_status_id in (2, 3)
                                    {cls.get_since_sql(since_datetime)}
                            '
                        ) as bs (
                            submission_id integer,
                            published_date timestamp,
                            certified_date timestamp,
                            toptier_code text,
                            reporting_start_date date,
                            reporting_end_date date,
                            reporting_fiscal_year integer,
                            reporting_fiscal_period integer,
                            is_quarter_format boolean
                        )
                        left outer join submission_attributes sa on
                            sa.submission_id = bs.submission_id
                ) t
            where
                new_certified_date is not null or
                anything_other_than_certified_date_has_changed
            order by
                published_date,
                submission_id
        """

        Submission = namedtuple("Submission", ["submission_id", "new_certified_date"])
        rows = execute_sql_to_named_tuple(sql)
        return (
            [
                Submission(r.submission_id, r.new_certified_date)
                for r in rows
                if r.new_certified_date and not r.anything_other_than_certified_date_has_changed
            ],
            [r.submission_id for r in rows if r.anything_other_than_certified_date_has_changed],
        )
