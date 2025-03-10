import logging

from django.conf import settings
from django.db import connection, connections

from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor
from usaspending_api.etl.management import load_base

logger = logging.getLogger("script")


class Command(load_base.Command):
    """
    This command will derive all FABS office names from the office codes in the Office table in Data broker. It
    will create a temporary Office table to JOIN on.
    """

    help = "Derives all FABS office names from the office codes in the Office table in Data broker. The \
                DATA_BROKER_DATABASE_URL environment variable must set so we can pull Office data from their db."

    def handle(self, *args, **options):
        # Grab data broker database connections
        if not options["test"]:
            try:
                db_conn = connections[settings.DATA_BROKER_DB_ALIAS]
                db_cursor = db_conn.cursor()
            except Exception as err:
                logger.critical("Could not connect to database. Is DATA_BROKER_DATABASE_URL set?")
                logger.critical(print(err))
                raise
        else:
            db_cursor = PhonyCursor()
        ds_cursor = connection.cursor()

        logger.info("Creating a temporary Office table copied from the Broker...")
        db_cursor.execute("SELECT office_name, office_code, sub_tier_code FROM office")
        all_offices = dictfetchall(db_cursor)
        all_offices_list = []
        for o in all_offices:
            office_name = o["office_name"].replace("'", "''")
            office_code = o["office_code"].replace("'", "''")
            sub_tier_code = o["sub_tier_code"].replace("'", "''")
            all_offices_list.append("('" + office_name + "','" + office_code + "','" + sub_tier_code + "')")
        all_offices_str = ", ".join(all_offices_list)

        ds_cursor.execute("CREATE TABLE temp_broker_office (office_name TEXT, office_code TEXT, sub_tier_code TEXT)")
        ds_cursor.execute(
            "INSERT INTO temp_broker_office (office_name, office_code, sub_tier_code) VALUES " + all_offices_str
        )

        logger.info("Deriving FABS awarding_office_names with awarding_office_codes from the temporary Office table...")
        ds_cursor.execute(
            "UPDATE transaction_fabs AS t_fabs "
            "SET awarding_office_name = office.office_name "
            "FROM temp_broker_office AS office "
            "WHERE t_fabs.awarding_office_code = office.office_code "
            "  AND t_fabs.action_date >= '2018-10-01' "
            "  AND t_fabs.awarding_office_name IS NULL "
            "  AND t_fabs.awarding_office_code IS NOT NULL"
        )
        logger.info(ds_cursor.rowcount)
        # logger.info('Made changes to {} records'.format(ds_cursor.results))

        logger.info("Deriving FABS funding_office_names with funding_office_codes from the temporary Office table...")
        ds_cursor.execute(
            "UPDATE transaction_fabs AS t_fabs "
            "SET funding_office_name = office.office_name "
            "FROM temp_broker_office AS office "
            "WHERE t_fabs.funding_office_code = office.office_code "
            "  AND t_fabs.action_date >= '2018-10-01' "
            "  AND t_fabs.funding_office_name IS NULL "
            "  AND t_fabs.funding_office_code IS NOT NULL"
        )
        logger.info(ds_cursor.rowcount)
        # logger.info('Made changes to {} records'.format(ds_cursor.results))

        logger.info(
            "Deriving FABS funding_sub_tier_agency_co, funding_sub_tier_agency_na, funding_agency_code, "
            "funding_agency_name from the temporary Office table..."
        )
        ds_cursor.execute(
            "WITH agency_list AS ("
            "  SELECT "
            "    tta.toptier_code AS agency_code,"
            "    tta.name AS agency_name,"
            "    sta.subtier_code AS sub_tier_code,"
            "    sta.name AS sub_tier_name"
            "  FROM agency AS a"
            "  INNER JOIN toptier_agency tta ON tta.toptier_agency_id = a.toptier_agency_id"
            "  INNER JOIN subtier_agency sta ON sta.subtier_agency_id = a.subtier_agency_id"
            "),"
            "office_list AS ("
            "  SELECT "
            "    office.office_code,"
            "    agency_list.agency_code,"
            "    agency_list.agency_name,"
            "    agency_list.sub_tier_code,"
            "    agency_list.sub_tier_name"
            "  FROM temp_broker_office AS office"
            "  INNER JOIN agency_list ON agency_list.sub_tier_code = office.sub_tier_code"
            ")"
            "UPDATE transaction_fabs AS t_fabs "
            "SET funding_sub_tier_agency_co = office_list.sub_tier_code,"
            "  funding_sub_tier_agency_na = office_list.sub_tier_name,"
            "  funding_agency_code = office_list.agency_code,"
            "  funding_agency_name = office_list.agency_name "
            "FROM office_list "
            "WHERE t_fabs.funding_office_code = office_list.office_code"
            "  AND t_fabs.action_date >= '2018/10/01'"
            "  AND t_fabs.funding_sub_tier_agency_co IS NULL"
            "  AND t_fabs.funding_office_code IS NOT NULL"
        )
        logger.info(ds_cursor.rowcount)
        # logger.info('Made changes to {} records'.format(ds_cursor.results))

        logger.info("Dropping temporary Office table...")
        ds_cursor.execute("DROP TABLE temp_broker_office")

        logger.info("Finished derivations.")
