"""
Jira Ticket Number(s): DEV-4027/DEV-4030

    Improve direct/reimbursable codes and correct 000 object class records.

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/fix_object_classes.py

Purpose:

    The original issue involved spreadsheet mangling issues with "000" class codes as documented in
    DEV-4030, but a couple other issues were identified in the process that are corrected by this
    script.

    Also, as per DEV-4027, direct/reimbursable should use a more obvious letter code rather than a number.

Life expectancy:

    Once Sprint 97 has been rolled out to production this script is safe to delete... although I
    would recommend keeping it around for a few additional sprints for reference.

"""
import logging
import psycopg2

from os import environ
from pathlib import Path


CONNECTION_STRING = environ["DATABASE_URL"]

OBJECT_CLASS_REMAPPING_SQL = """
    with object_class_mapping as (
        select  boc.id as bad_object_class_id,
                goc.id as good_object_class_id
        from    object_class as boc
                inner join object_class as goc on
                    goc.major_object_class = boc.major_object_class and
                    goc.direct_reimbursable is not distinct from boc.direct_reimbursable
        where   boc.object_class = '0' and
                goc.object_class = '000'
    )
    update  {} as t
    set     object_class_id = ocm.good_object_class_id
    from    object_class_mapping as ocm
    where   t.object_class_id = ocm.bad_object_class_id
"""

STEPS = (
    (
        "Delete nonsensical 90/000 object class",
        "delete from object_class where major_object_class = '90' and object_class = '000'",
    ),
    (
        "Remap '0' financial_accounts_by_awards records to '000'",
        OBJECT_CLASS_REMAPPING_SQL.format("financial_accounts_by_awards"),
    ),
    (
        "Remap '0' financial_accounts_by_program_activity_object_class records to '000'",
        OBJECT_CLASS_REMAPPING_SQL.format("financial_accounts_by_program_activity_object_class"),
    ),
    (
        "Remap '0' tas_program_activity_object_class_quarterly records to '000'",
        OBJECT_CLASS_REMAPPING_SQL.format("tas_program_activity_object_class_quarterly"),
    ),
    ("Delete '0' object classes", "delete from object_class where object_class = '0'"),
    (
        "Update object class '000' names",
        """
            update  object_class
            set     major_object_class_name = 'Unknown', object_class_name = 'Unknown'
            where   object_class = '000' and (
                        major_object_class_name is distinct from 'Unknown' or
                        object_class_name is distinct from 'Unknown'
                    )
        """,
    ),
    (
        "Recode direct_reimbursable '1' to 'D'",
        """
            update  object_class
            set     direct_reimbursable = 'D', direct_reimbursable_name = 'Direct'
            where   direct_reimbursable = '1'
        """,
    ),
    (
        "Recode direct_reimbursable '2' to 'R'",
        """
            update  object_class
            set     direct_reimbursable = 'R', direct_reimbursable_name = 'Reimbursable'
            where   direct_reimbursable = '2'
        """,
    ),
    (
        "Ensure unknown direct_reimbursables are good",
        """
            update  object_class
            set     direct_reimbursable = null, direct_reimbursable_name = null
            where   direct_reimbursable is distinct from 'D' and
                    direct_reimbursable is distinct from 'R' and (
                        direct_reimbursable is not null or
                        direct_reimbursable_name is not null
                    )
        """,
    ),
)


logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z"
)


# Import our USAspending Timer component.  This will not work if we ever add
# any Django specific stuff to the timing_helpers.py file.
exec(Path("usaspending_api/common/helpers/timing_helpers.py").read_text())


# Simplify instantiations of Timer to automatically use the correct logger.
class Timer(Timer):  # noqa - because we're using trickery to import this
    def __init__(self, message=None):
        super().__init__(message=message, success_logger=logging.info, failure_logger=logging.error)


def execute(sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        if cursor.rowcount > -1:
            logging.info(f"{cursor.rowcount:,} rows affected")


def run_steps():
    for step in STEPS:
        with Timer(step[0]):
            execute(step[1])


def vacuum_tables():
    with Timer("Vacuuming object_class table"):
        execute("vacuum (full, analyze) object_class")
    with Timer("Vacuuming financial_accounts_by_awards table"):
        execute("vacuum analyze financial_accounts_by_awards")
    with Timer("Vacuuming financial_accounts_by_program_activity_object_class table"):
        execute("vacuum analyze financial_accounts_by_program_activity_object_class")
    with Timer("Vacuuming tas_program_activity_object_class_quarterly table"):
        execute("vacuum analyze tas_program_activity_object_class_quarterly")


if __name__ == "__main__":
    with Timer("Overall run"):
        with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
            run_steps()
            with Timer("Committing transaction"):
                connection.commit()
            connection.autocommit = True
            vacuum_tables()
