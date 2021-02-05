from django.db import connection


RECENT_PERIOD_SQL = """
SELECT
    submission_fiscal_month,
    submission_fiscal_year,
    is_quarter,
    submission_reveal_date
FROM
    dabs_submission_window_schedule
WHERE
    is_quarter = {is_quarter}
    AND submission_reveal_date <= NOW()
ORDER BY
    submission_fiscal_year DESC,
    submission_fiscal_month DESC
LIMIT 2
"""


def retrieve_recent_periods():
    # Open connection to database
    with connection.cursor() as cursor:

        # Query for most recent closed month periods
        cursor.execute(RECENT_PERIOD_SQL.format(is_quarter="FALSE"))
        recent_month_periods = cursor.fetchmany(2)

        # Query for most recent closed quarter periods
        cursor.execute(RECENT_PERIOD_SQL.format(is_quarter="TRUE"))
        recent_quarter_periods = cursor.fetchmany(2)

    recent_periods = {
        "this_month": read_period_fields(recent_month_periods[0]),
        "this_quarter": read_period_fields(recent_quarter_periods[0]),
        "last_month": read_period_fields(recent_month_periods[1]),
        "last_quarter": read_period_fields(recent_quarter_periods[1]),
    }

    return recent_periods


def read_period_fields(period):
    return {"month": period[0], "year": period[1], "is_quarter": period[2], "submission_reveal_date": period[3]}
