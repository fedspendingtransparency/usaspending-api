#!/usr/bin/env python3
"""
Jira Ticket Number(s): DEV-3759

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/fix_awards_earliest_latest_transactions.py

Purpose:

    Update awards where the earliest transaction, latest transaction, or officers is incorrect
    according to new sorting algorithm.

"""
import logging
import math
import psycopg2
import time

from datetime import timedelta
from os import environ


logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s", datefmt="%Y/%m/%d %H:%M:%S (%Z)"
)


CONNECTION_STRING = environ["DATABASE_URL"]


SQLS = (
    (
        "Extract pertinent transaction data",
        """
            create temporary table
                temp_dev_3759_transaction_data
            as
                select
                    tn.award_id,
                    tn.id as transaction_id,
                    tn.action_date,
                    tn.modification_number,
                    tn.transaction_unique_id,
                    tn.description,
                    tn.period_of_performance_start_date,
                    coalesce(fabs.officer_1_amount, fpds.officer_1_amount) as officer_1_amount,
                    coalesce(fabs.officer_1_name, fpds.officer_1_name) as officer_1_name,
                    coalesce(fabs.officer_2_amount, fpds.officer_2_amount) as officer_2_amount,
                    coalesce(fabs.officer_2_name, fpds.officer_2_name) as officer_2_name,
                    coalesce(fabs.officer_3_amount, fpds.officer_3_amount) as officer_3_amount,
                    coalesce(fabs.officer_3_name, fpds.officer_3_name) as officer_3_name,
                    coalesce(fabs.officer_4_amount, fpds.officer_4_amount) as officer_4_amount,
                    coalesce(fabs.officer_4_name, fpds.officer_4_name) as officer_4_name,
                    coalesce(fabs.officer_5_amount, fpds.officer_5_amount) as officer_5_amount,
                    coalesce(fabs.officer_5_name, fpds.officer_5_name) as officer_5_name,
                    tn.awarding_agency_id,
                    tn.funding_agency_id,
                    tn.last_modified_date,
                    tn.period_of_performance_current_end_date,
                    tn.place_of_performance_id,
                    tn.recipient_id,
                    tn.type,
                    tn.type_description,
                    CASE WHEN tn.type IN ('A', 'B', 'C', 'D') THEN 'contract'
                    WHEN tn.type IN ('02', '03', '04', '05') THEN 'grant'
                    WHEN tn.type in ('06', '10') THEN 'direct payment'
                    WHEN tn.type in ('07', '08') THEN 'loans'
                    WHEN tn.type = '09' THEN 'insurance'
                    WHEN tn.type = '11' THEN 'other'
                    WHEN tn.type LIKE 'IDV%%' THEN 'idv'
                    ELSE NULL END AS category
                from
                    transaction_normalized as tn
                    left outer join transaction_fabs fabs on fabs.transaction_id = tn.id
                    left outer join transaction_fpds fpds on fpds.transaction_id = tn.id
        """,
    ),  # 17 minutes
    (
        "Index pertinent transaction data",
        """
            create index
                temp_dev_3759_transaction_data_ix1
            on
                temp_dev_3759_transaction_data (
                    award_id,
                    action_date,
                    modification_number,
                    transaction_unique_id,
                    transaction_id
                )
        """,
    ),  # 3 minutes
    (
        "Index pertinent transaction data the other way",
        """
            create index
                temp_dev_3759_transaction_data_ix2
            on
                temp_dev_3759_transaction_data (
                    award_id,
                    action_date desc,
                    modification_number desc,
                    transaction_unique_id desc,
                    transaction_id desc
                )
        """,
    ),  # 3 minutes
    (
        "Determine earliest transaction id",
        """
            create temporary table
                temp_dev_3759_earliest_transaction
            as
                select distinct on (award_id)
                    award_id,
                    transaction_id
                from
                    temp_dev_3759_transaction_data
                order by
                    award_id,
                    action_date asc,
                    modification_number asc,
                    transaction_unique_id asc
        """,
    ),
    (
        "Index earliest transaction",
        """
            create unique index
                temp_dev_3759_earliest_transaction_ix
            on
                temp_dev_3759_earliest_transaction (award_id)
        """,
    ),
    (
        "Determine latest transaction id",
        """
            create temporary table
                temp_dev_3759_latest_transaction
            as
                select distinct on (award_id)
                    award_id,
                    transaction_id
                from
                    temp_dev_3759_transaction_data
                order by
                    award_id,
                    action_date desc,
                    modification_number desc,
                    transaction_unique_id desc
        """,
    ),
    (
        "Index latest transaction",
        """
            create unique index
                temp_dev_3759_latest_transaction_ix
            on
                temp_dev_3759_latest_transaction (award_id)
        """,
    ),
    (
        "Determine executive compensation",
        """
            create temporary table
                temp_dev_3759_executive_compensation
            as
                select distinct on (award_id)
                    award_id,
                    officer_1_amount,
                    officer_1_name,
                    officer_2_amount,
                    officer_2_name,
                    officer_3_amount,
                    officer_3_name,
                    officer_4_amount,
                    officer_4_name,
                    officer_5_amount,
                    officer_5_name
                from
                    temp_dev_3759_transaction_data
                where
                    officer_1_name is not null
                order by
                    award_id,
                    action_date desc,
                    modification_number desc,
                    transaction_unique_id desc
        """,
    ),
    (
        "Index executive compensation",
        """
            create unique index
                temp_dev_3759_executive_compensation_ix
            on
                temp_dev_3759_executive_compensation (award_id)
        """,
    ),
    (
        "Update awards",
        """
            update
                awards as a
            set
                earliest_transaction_id = e.transaction_id,
                date_signed = e.action_date,
                description = e.description,
                period_of_performance_start_date = e.period_of_performance_start_date,
                latest_transaction_id = l.transaction_id,
                awarding_agency_id = l.awarding_agency_id,
                category = l.category,
                certified_date = l.action_date,
                funding_agency_id = l.funding_agency_id,
                last_modified_date = l.last_modified_date,
                period_of_performance_current_end_date = l.period_of_performance_current_end_date,
                place_of_performance_id = l.place_of_performance_id,
                recipient_id = l.recipient_id,
                type = l.type,
                type_description = l.type_description,
                officer_1_amount = ec.officer_1_amount,
                officer_1_name = ec.officer_1_name,
                officer_2_amount = ec.officer_2_amount,
                officer_2_name = ec.officer_2_name,
                officer_3_amount = ec.officer_3_amount,
                officer_3_name = ec.officer_3_name,
                officer_4_amount = ec.officer_4_amount,
                officer_4_name = ec.officer_4_name,
                officer_5_amount = ec.officer_5_amount,
                officer_5_name = ec.officer_5_name
            from
                temp_dev_3759_earliest_transaction as et
                inner join temp_dev_3759_transaction_data as e on e.transaction_id = et.transaction_id
                inner join temp_dev_3759_latest_transaction as lt on lt.award_id = et.award_id
                inner join temp_dev_3759_transaction_data as l on l.transaction_id = lt.transaction_id
                left outer join temp_dev_3759_executive_compensation ec on ec.award_id = et.award_id
            where
                a.id = et.award_id and (
                    a.earliest_transaction_id is distinct from e.transaction_id or
                    a.date_signed is distinct from e.action_date or
                    a.description is distinct from e.description or
                    a.period_of_performance_start_date is distinct from e.period_of_performance_start_date or
                    a.latest_transaction_id is distinct from l.transaction_id or
                    a.awarding_agency_id is distinct from l.awarding_agency_id or
                    a.category is distinct from l.category or
                    a.certified_date is distinct from l.action_date or
                    a.funding_agency_id is distinct from l.funding_agency_id or
                    a.last_modified_date is distinct from l.last_modified_date or
                    a.period_of_performance_current_end_date is distinct from l.period_of_performance_current_end_date or
                    a.place_of_performance_id is distinct from l.place_of_performance_id or
                    a.recipient_id is distinct from l.recipient_id or
                    a.type is distinct from l.type or
                    a.type_description is distinct from l.type_description or
                    a.officer_1_amount is distinct from ec.officer_1_amount or
                    a.officer_1_name is distinct from ec.officer_1_name or
                    a.officer_2_amount is distinct from ec.officer_2_amount or
                    a.officer_2_name is distinct from ec.officer_2_name or
                    a.officer_3_amount is distinct from ec.officer_3_amount or
                    a.officer_3_name is distinct from ec.officer_3_name or
                    a.officer_4_amount is distinct from ec.officer_4_amount or
                    a.officer_4_name is distinct from ec.officer_4_name or
                    a.officer_5_amount is distinct from ec.officer_5_amount or
                    a.officer_5_name is distinct from ec.officer_5_name
                )
        """,
    ),
)


class Timer:
    """ A "lite" version of the Timer from usaspending_api/common/helpers/timing_helpers.py. """

    _formats = "{:,} d", "{} h", "{} m", "{} s", "{} ms"

    def __init__(self, message=None):
        self.message = message

    def __enter__(self):
        logging.info("{} starting...".format(self.message))
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop = time.perf_counter()
        self._elapsed = timedelta(seconds=(self._stop - self._start))
        if exc_type is None:
            logging.info("{} finished successfully after {}".format(self.message, self))
        else:
            logging.error("{} FAILED AFTER {}".format(self.message, self))

    def __repr__(self):
        f, s = math.modf(self._elapsed.total_seconds())
        ms = round(f * 1000)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)
        return (
            " ".join(f.format(b) for f, b in zip(self._formats, tuple(int(n) for n in (d, h, m, s, ms))) if b > 0)
            or "less than a millisecond"
        )


def main():
    with Timer("Fix awards (overall)"):
        with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
            for sql in SQLS:
                with Timer(sql[0]):
                    with connection.cursor() as cursor:
                        cursor.execute(sql[1])
                        rowcount = cursor.rowcount
                    if rowcount > -1:
                        logging.info("    {:,} rows affected".format(rowcount))
            with Timer("Committing transaction"):
                connection.commit()


if __name__ == "__main__":
    main()
