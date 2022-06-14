from django.conf import settings
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_named_tuple


def get_new_or_updated_submission_ids(since_datetime=None):
    """
    Identifies Broker submissions that need to be created or updated in USAspending and returns a
    list of their ids.
    """
    since_sql = f"and s.updated_at >= ''{since_datetime}''::timestamp" if since_datetime else ""
    sql = f"""
        select
            bs.submission_id
        from
            dblink(
                '{settings.DATA_BROKER_DBLINK_NAME}',
                '
                    select
                        s.submission_id,
                        (
                            select  max(updated_at)
                            from    publish_history
                            where   submission_id = s.submission_id
                        )::timestamptz as published_date,
                        (
                            select  max(updated_at)
                            from    certify_history
                            where   submission_id = s.submission_id
                        )::timestamptz as certified_date,
                        coalesce(s.cgac_code, s.frec_code) as toptier_code,
                        s.reporting_start_date,
                        s.reporting_end_date,
                        s.reporting_fiscal_year,
                        s.reporting_fiscal_period,
                        s.is_quarter_format
                    from
                        submission as s
                    where
                        s.is_fabs is false
                        and s.publish_status_id in (2, 3)
                        {since_sql}
                '
            ) as bs (
                submission_id integer,
                published_date timestamp with time zone,
                certified_date timestamp with time zone,
                toptier_code text,
                reporting_start_date date,
                reporting_end_date date,
                reporting_fiscal_year integer,
                reporting_fiscal_period integer,
                is_quarter_format boolean
            )
            left outer join submission_attributes sa on
                sa.submission_id = bs.submission_id
                and sa.published_date is not distinct from bs.published_date
                and sa.certified_date is not distinct from bs.certified_date
                and sa.toptier_code is not distinct from bs.toptier_code
                and sa.reporting_period_start is not distinct from bs.reporting_start_date
                and sa.reporting_period_end is not distinct from bs.reporting_end_date
                and sa.reporting_fiscal_year is not distinct from bs.reporting_fiscal_year
                and sa.reporting_fiscal_period is not distinct from bs.reporting_fiscal_period
                and sa.quarter_format_flag is not distinct from bs.is_quarter_format
        where
            sa.submission_id is null
    """

    rows = execute_sql_to_named_tuple(sql)
    return [r.submission_id for r in rows]
