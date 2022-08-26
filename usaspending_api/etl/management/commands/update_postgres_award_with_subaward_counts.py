# TODO: Remove this script and use update_delta_award_with_subaward_counts when rpt.award_search includes the columns

import logging
import psycopg2

from django.core.management.base import BaseCommand
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


class Command(BaseCommand):

    help = """
    This command simply updates the awards data on postgres with subaward counts based on rpt.subaward_search
    """

    def handle(self, *args, **options):
        update_award_query = f"""
            WITH subaward_totals AS (
                SELECT
                    award_id,
                    sum(COALESCE(subaward_amount, 0)) AS total_subaward_amount,
                    count(*) as subaward_count
                FROM
                    rpt.subaward_search
                GROUP BY
                    award_id
            )
            UPDATE
                rpt.awards AS a1
            SET
                update_date = NOW(),
                total_subaward_amount = st.total_subaward_amount,
                subaward_count = COALESCE(st.subaward_count, 0)
            FROM
                rpt.awards AS a
            LEFT OUTER JOIN
                subaward_totals AS st
                    ON st.award_id = a.id
            WHERE
                a.id = a1.id
                AND (
                    st.total_subaward_amount IS DISTINCT FROM a1.total_subaward_amount
                    OR COALESCE(st.subaward_count, 0) IS DISTINCT FROM a1.subaward_count
                );
        """
        with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
            with connection.cursor() as cursor:
                logger.info("Updating rpt.awards based on rpt.subaward_search.")
                cursor.execute(update_award_query)
                logger.info("rpt.awards updated.")
