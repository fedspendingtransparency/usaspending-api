import logging
from datetime import datetime, timezone

from django.db import connection
from django.core.management.base import BaseCommand

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.etl.management.helpers.recent_periods import retrieve_recent_periods

logger = logging.getLogger("script")

TOUCH_AWARD_SQL = """
UPDATE award_search
SET update_date = now()
WHERE award_id IN (
    SELECT DISTINCT faba.award_id
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc ON
        defc.group_name = 'covid_19'
        AND faba.disaster_emergency_fund_code = defc.code
    INNER JOIN submission_attributes sa ON
        faba.submission_id = sa.submission_id
    INNER JOIN dabs_submission_window_schedule dabs ON
        dabs.id = sa.submission_window_id
        AND dabs.submission_fiscal_year = {year}
        AND dabs.submission_fiscal_month = {month}
        AND dabs.is_quarter = {is_quarter}
        AND faba.award_id IS NOT null
);
"""


class Command(BaseCommand):
    """
    NOTE: This command is necessary to update award values in Elasticsearch that were modified in the
    recently closed submission period. Elasticsearch calculations for an award's Covid-19 spending
    values do not include data from a submission until its submission window's reveal date has been
    passed. Because an incremental load in Elasticsearch uses the award's update_date to determine the
    delta since it last ran, the award data will not be refreshed after a submission closes until it's
    update_date is touched. This command updates the update_date field on awards modified by the latest
    closed submission, so the Elasticsearch incremental load will include those awards. The command
    uses the external_data_load_date table to make sure each submission window is only 'revealed' once.
    """

    def handle(self, *args, **options):
        script_start_time = datetime.now(timezone.utc)
        periods = retrieve_recent_periods()

        # Using `script_start_time` as a default, so no awards will be touched the first time this script
        # is run. The assumption is that awards are up to date at the time the script is deployed. After
        # this runs the first time, a date will be populated in the database.
        self.last_load_date = get_last_load_date("touch_last_period_awards", default=script_start_time)

        logger.info(f"Using {script_start_time} to determine if awards should be touched.")

        total_records_updated = 0

        total_records_updated += self.touch_period_awards_if_behind(periods["this_month"])
        total_records_updated += self.touch_period_awards_if_behind(periods["this_quarter"])

        update_last_load_date("touch_last_period_awards", script_start_time)

        logger.info(f"Found {total_records_updated:,} award records to update in Elasticsearch")

        # Return will be captured as stdout in Jenkins job
        return str(total_records_updated)

    def touch_period_awards_if_behind(self, period):

        records_updated = 0

        year = period["year"]
        month = period["month"]
        is_quarter = period["is_quarter"]

        is_quarter_str = "quarter" if is_quarter else "month"

        if self.last_load_date < period["submission_reveal_date"]:
            formatted_sql = TOUCH_AWARD_SQL.format(year=year, month=month, is_quarter=is_quarter)

            with connection.cursor() as cursor:
                cursor.execute(formatted_sql)
                records_updated = cursor.rowcount

            logger.info(
                f"Touching {records_updated:,} award updates from {is_quarter_str} - year: {year}, period: {month}"
            )
        else:
            logger.info(f"Awards have already been touched from {is_quarter_str} - year: {year}, period: {month}")

        return records_updated
