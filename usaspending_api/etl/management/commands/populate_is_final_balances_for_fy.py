import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("script")

POPULATE_FINAL_BALANCES_FOR_FY_SQL = """
WITH final_balance_submissions AS (
    SELECT
        DISTINCT ON
        (
            sa.toptier_code, sa.reporting_fiscal_year
        ) sa.submission_id
    FROM
        submission_attributes sa
    INNER JOIN (
            SELECT
                DISTINCT ON
                (
                    is_quarter, submission_fiscal_year
                ) submission_fiscal_year, submission_fiscal_month, is_quarter, submission_due_date, id
            FROM
                dabs_submission_window_schedule
            WHERE
                submission_reveal_date < now()
            ORDER BY
                is_quarter, submission_fiscal_year DESC, submission_fiscal_month DESC
        ) AS latest_closed_periods_per_fy ON
        sa.submission_window_id = latest_closed_periods_per_fy.id
    ORDER BY
        sa.toptier_code, sa.reporting_fiscal_year, latest_closed_periods_per_fy.submission_due_date DESC
)
UPDATE
    submission_attributes
SET
    is_final_balances_for_fy = submission_id IN (SELECT submission_id FROM final_balance_submissions),
    update_date = NOW()
WHERE
    is_final_balances_for_fy != submission_id IN (SELECT submission_id FROM final_balance_submissions);
"""


class Command(BaseCommand):
    help = (
        "This command populates the 'is_final_balances_for_fy' field of submission_attributes."
        "This field is set to true for submissions associated with the latest closed submission"
        "for any fiscal year. NOTE: It is possible that an agency hasn't made a submission for "
        "the latest period of a fiscal year."
    )

    def handle(self, *args, **options):

        # Open connection to database
        with connection.cursor() as cursor:

            # Queries to populate 'is_final_balances_for_fy' field in 'submission_attributes' table
            cursor.execute(POPULATE_FINAL_BALANCES_FOR_FY_SQL)
