import logging

from datetime import timedelta
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Max
from usaspending_api.submissions.models import SubmissionAttributes


logger = logging.getLogger("script")


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--submission-ids",
            help=(
                "Optionally supply one or more Broker submission_ids to be created or updated.  If this "
                "parameter is omitted, an incremental load is performed."
            ),
            nargs="+",
            type=int,
        )
        parser.add_argument(
            "--list-ids-only",
            action="store_true",
            help="Only list submissions to be loaded.  Do not actually load them.",
        )

    def handle(self, *args, **options):

        if options["submission_ids"]:
            submission_ids = options["submission_ids"]
        else:
            submission_ids = self.get_incremental_submission_ids()

        if submission_ids:
            msg = f"{len(submission_ids):,} submissions will be created or updated"
            if len(submission_ids) <= 1000:
                logger.info(f"The following {msg}: {submission_ids}")
            else:
                logger.info(f"{msg}.")
            if options["list_ids_only"]:
                logger.info("Exiting script before data load occurs in accordance with the --list-ids-only flag.")
                return
        else:
            logger.info("There are no new or updated submissions to load.")
            return

        failed_submissions = []
        for submission_id in submission_ids:
            try:
                call_command("load_submission", submission_id)
            except SystemExit:
                logger.info(f"Submission failed to load: {submission_id}")
                failed_submissions.append(submission_id)
            except Exception:
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
    def calculate_since_datetime():
        since = SubmissionAttributes.objects.all().aggregate(Max("update_date"))["update_date__max"]
        if since is None:
            logger.info("No records found in submission_attributes.  Performing a full load.")
        else:
            # In order to prevent skips, we're just always going to look back 7 days.  Since submission is a
            # relatively low volume table, this should not cause any noticeable performance issues.
            since -= timedelta(days=14)
            logger.info(f"Performing incremental load starting from {since}.")
        return since

    def get_incremental_submission_ids(self):
        since = self.calculate_since_datetime()
        since = f"and s.updated_at >= ''{since}''::timestamp" if since else ""
        # Note that this is designed to work with our conservative lookback period by filtering
        # out rows that haven't changed.
        sql = f"""
            select
                bs.submission_id
            from
                dblink(
                    '{settings.DATA_BROKER_DBLINK_NAME}',
                    '
                        select
                            s.submission_id,
                            s.updated_at::date as certified_date,
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
                            s.publish_status_id in (2, 3) and
                            exists(select from certify_history where submission_id = s.submission_id)
                            {since}
                    '
                ) as bs (
                    submission_id integer,
                    certified_date date,
                    toptier_code text,
                    reporting_start_date date,
                    reporting_end_date date,
                    reporting_fiscal_year integer,
                    reporting_fiscal_period integer,
                    is_quarter_format boolean
                )
                left outer join submission_attributes sa on
                    sa.submission_id = bs.submission_id and
                    sa.certified_date is not distinct from bs.certified_date and
                    sa.toptier_code is not distinct from bs.toptier_code and
                    sa.reporting_period_start is not distinct from bs.reporting_start_date and
                    sa.reporting_period_end is not distinct from bs.reporting_end_date and
                    sa.reporting_fiscal_period is not distinct from bs.reporting_fiscal_period and
                    sa.quarter_format_flag is not distinct from bs.is_quarter_format
            where
                sa.submission_id is null
            order by
                bs.submission_id
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return [s[0] for s in cursor.fetchall()]
