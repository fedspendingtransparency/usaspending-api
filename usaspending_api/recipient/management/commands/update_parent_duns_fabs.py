import logging
import time

from django.db import connection
from django.core.management.base import BaseCommand

logger = logging.getLogger("script")

FABS_PARENT_DUNS_SQL_MATCH = """
    WITH joined_historical_fabs AS (
        SELECT
            hfabs.published_fabs_id AS "fabs_id",
            hpd.ultimate_parent_unique_ide AS "parent_duns",
            hpd.ultimate_parent_legal_enti AS "parent_name"
        FROM transaction_fabs hfabs
        JOIN historic_parent_duns hpd ON (
            hfabs.awardee_or_recipient_uniqu=hpd.awardee_or_recipient_uniqu AND
            EXTRACT(YEAR FROM CAST(hfabs.action_date AS DATE))=hpd.year
        )
        WHERE (
            hfabs.ultimate_parent_unique_ide IS NULL
        )
    )
    UPDATE transaction_fabs AS fabs
    SET
       ultimate_parent_unique_ide = joined_historical_fabs.parent_duns,
       ultimate_parent_legal_enti = joined_historical_fabs.parent_name
    FROM joined_historical_fabs
    WHERE
       joined_historical_fabs.fabs_id = fabs.published_fabs_id
"""

CREATE_TEMP_MIN_YEARS_MATVIEW = """
    CREATE MATERIALIZED VIEW tmp_matview_min_years
    AS (
        SELECT awardee_or_recipient_uniqu, MIN(year) as "min_year"
        FROM historic_parent_duns
        GROUP BY awardee_or_recipient_uniqu
    );

    CREATE INDEX min_years_min_year_idx ON tmp_matview_min_years (min_year);
    CREATE INDEX min_years_awardee_or_recipient_uniqu_idx ON tmp_matview_min_years (awardee_or_recipient_uniqu);
"""

FABS_PARENT_DUNS_SQL_EARLIEST = """
    WITH joined_historical_fabs AS (
        SELECT
            hfabs.published_fabs_id AS "fabs_id",
            hpd.ultimate_parent_unique_ide AS "parent_duns",
            hpd.ultimate_parent_legal_enti AS "parent_name"
        FROM transaction_fabs hfabs
        JOIN historic_parent_duns hpd ON (
            hfabs.awardee_or_recipient_uniqu=hpd.awardee_or_recipient_uniqu
        )
        JOIN tmp_matview_min_years min_years ON (
            hfabs.awardee_or_recipient_uniqu = min_years.awardee_or_recipient_uniqu
        )
        WHERE (
            hpd.year = min_years.min_year AND
            hfabs.ultimate_parent_unique_ide IS NULL
        )
    )
    UPDATE transaction_fabs AS fabs
    SET
       ultimate_parent_unique_ide = joined_historical_fabs.parent_duns,
       ultimate_parent_legal_enti = joined_historical_fabs.parent_name
    FROM joined_historical_fabs
    WHERE
       joined_historical_fabs.fabs_id = fabs.published_fabs_id;
"""

DROP_TEMP_MIN_YEARS_MATVIEW = """
    DROP INDEX min_years_min_year_idx;
    DROP INDEX min_years_awardee_or_recipient_uniqu_idx;
    DROP MATERIALIZED VIEW tmp_matview_min_years;
"""


class Command(BaseCommand):
    help = "Loads state data from Census data"

    def handle(self, *args, **options):
        with connection.cursor() as curs:
            logger.info("Updating FABS with action dates matching the years within the parent duns")
            start = time.time()
            curs.execute(FABS_PARENT_DUNS_SQL_MATCH)
            logger.info("Updating FABS with matching action dates took {} seconds".format(time.time() - start))

            logger.info("Updating FABS with action dates not matching the parent duns, using the earliest match")
            start = time.time()
            curs.execute(CREATE_TEMP_MIN_YEARS_MATVIEW)
            curs.execute(FABS_PARENT_DUNS_SQL_EARLIEST)
            curs.execute(DROP_TEMP_MIN_YEARS_MATVIEW)
            logger.info("Updating FABS with non-matching action dates took {} seconds".format(time.time() - start))
        logger.info("Updating FABS complete")
