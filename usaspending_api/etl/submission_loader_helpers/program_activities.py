from django.conf import settings
from django.db import connection

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import RefProgramActivity


PROGRAM_ACTIVITIES = {}


def update_program_activities(submission_id: int, single_writer: bool = False) -> int:
    """
    Grab from Broker all of the program activity info for the indicated submission and stuff it
    into USAspending.

    single_writer=False (default): Other load_submission processes may be running concurrently (e.g. under
    'load_multiple_submissions') and could insert a program activity we don't know about, so we unconditionally reload
    the entire ref_program_activity table to guarantee we have everything this submission needs.

    single_writer=True: The caller guarantees no sibling process can be running right now, so we can UPSERT-and-RETURN
    exactly the rows this submission needs in one statement instead of reloading the whole table.
    """

    if single_writer:
        sql = f"""
            INSERT INTO ref_program_activity (
                program_activity_code,
                program_activity_name,
                budget_year,
                responsible_agency_id,
                allocation_transfer_agency_id,
                main_account_code,
                create_date,
                update_date
            )
            SELECT
                program_activity_code,
                program_activity_name,
                reporting_fiscal_year,
                agency_identifier,
                allocation_transfer_agency,
                main_account_code,
                now(),
                now()
            FROM
                DBLINK(
                    '{settings.BROKER_DBLINK_NAME}',
                    '
                        select b.program_activity_code, upper(b.program_activity_name) program_activity_name,
                               s.reporting_fiscal_year, b.agency_identifier, b.allocation_transfer_agency, b.main_account_code
                        from published_object_class_program_activity b
                            inner join submission s on s.submission_id = b.submission_id
                        where s.submission_id = {submission_id} and b.program_activity_code is not null
                        union
                        select c.program_activity_code, upper(c.program_activity_name) program_activity_name,
                               s.reporting_fiscal_year, c.agency_identifier, c.allocation_transfer_agency, c.main_account_code
                        from published_award_financial c
                            inner join submission s on s.submission_id = c.submission_id
                        where s.submission_id = {submission_id} and c.program_activity_code is not null
                              and ((c.transaction_obligated_amou is not null and c.transaction_obligated_amou != 0)
                                or (c.gross_outlay_amount_by_awa_cpe is not null and c.gross_outlay_amount_by_awa_cpe != 0))
                    '
                ) AS bs (
                    program_activity_code text,
                    program_activity_name text,
                    reporting_fiscal_year text,
                    agency_identifier text,
                    allocation_transfer_agency text,
                    main_account_code text
                )
            ON CONFLICT (
                program_activity_code,
                program_activity_name,
                responsible_agency_id,
                allocation_transfer_agency_id,
                main_account_code,
                budget_year
            )
            DO UPDATE SET update_date = now()
            RETURNING
                id,
                program_activity_code,
                program_activity_name,
                budget_year,
                responsible_agency_id,
                allocation_transfer_agency_id,
                main_account_code,
                (xmax = 0) AS inserted
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = dictfetchall(cursor)

        PROGRAM_ACTIVITIES.update(
            {
                (
                    row["program_activity_code"],
                    row["program_activity_name"],
                    row["budget_year"],
                    row["responsible_agency_id"],
                    row["allocation_transfer_agency_id"],
                    row["main_account_code"],
                ): RefProgramActivity(
                    id=row["id"],
                    program_activity_code=row["program_activity_code"],
                    program_activity_name=row["program_activity_name"],
                    budget_year=row["budget_year"],
                    responsible_agency_id=row["responsible_agency_id"],
                    allocation_transfer_agency_id=row["allocation_transfer_agency_id"],
                    main_account_code=row["main_account_code"],
                )
                for row in rows
            }
        )
        return sum(row["inserted"] for row in rows)

    global PROGRAM_ACTIVITIES

    sql = f"""
        insert into ref_program_activity (
                program_activity_code,
                program_activity_name,
                budget_year,
                responsible_agency_id,
                allocation_transfer_agency_id,
                main_account_code,
                create_date,
                update_date
            )
        select
            program_activity_code,
            program_activity_name,
            reporting_fiscal_year,
            agency_identifier,
            allocation_transfer_agency,
            main_account_code,
            now(),
            now()
        from
            dblink(
                '{settings.BROKER_DBLINK_NAME}',
                '
                    select  b.program_activity_code,
                            upper(b.program_activity_name) program_activity_name,
                            s.reporting_fiscal_year,
                            b.agency_identifier,
                            b.allocation_transfer_agency,
                            b.main_account_code
                    from    published_object_class_program_activity b
                            inner join submission s on s.submission_id = b.submission_id
                    where   s.submission_id = {submission_id}
                            and b.program_activity_code is not null

                    union

                    select  c.program_activity_code,
                            upper(c.program_activity_name) program_activity_name,
                            s.reporting_fiscal_year,
                            c.agency_identifier,
                            c.allocation_transfer_agency,
                            c.main_account_code
                    from    published_award_financial c
                            inner join submission s on s.submission_id = c.submission_id
                    where   s.submission_id = {submission_id}
                            and c.program_activity_code is not null
                            and (
                                (
                                    c.transaction_obligated_amou is not null and
                                    c.transaction_obligated_amou != 0
                                ) or (
                                    c.gross_outlay_amount_by_awa_cpe is not null and
                                    c.gross_outlay_amount_by_awa_cpe != 0
                                )
                            )
                '
            ) as bs (
                program_activity_code text,
                program_activity_name text,
                reporting_fiscal_year text,
                agency_identifier text,
                allocation_transfer_agency text,
                main_account_code text
            )
        on conflict do nothing
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount

    PROGRAM_ACTIVITIES = {
        (
            pa.program_activity_code,
            pa.program_activity_name,
            pa.budget_year,
            pa.responsible_agency_id,
            pa.allocation_transfer_agency_id,
            pa.main_account_code,
        ): pa
        for pa in RefProgramActivity.objects.all()
    }

    return rowcount


def get_program_activity(row, submission_attributes):
    if row["program_activity_code"] is None:
        return None
    key = (
        row["program_activity_code"],
        row["program_activity_name"].upper() if row["program_activity_name"] else row["program_activity_name"],
        str(submission_attributes.reporting_fiscal_year),
        row["agency_identifier"],
        row["allocation_transfer_agency"],
        row["main_account_code"],
    )
    return PROGRAM_ACTIVITIES[key]
