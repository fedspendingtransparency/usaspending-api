import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("console")


class Command(BaseCommand):
    @staticmethod
    def create_matviews(sess):
        logger.info("Creating zip_county temporary view")

        logger.info("Created zip_county temporary view, creating zip_county index on zip5")
        sess.execute("CREATE INDEX ix_zip5_zip_county ON zip_county (zip5)")

        logger.info("Created zip_county index on zip5, creating zip_county index on zip_last4")
        sess.execute("CREATE INDEX ix_zip_last4_zip_county ON zip_county (zip_last4)")

        logger.info("Created zip_county index on zip_last4, creating zip_county index on combined_zip")
        sess.execute("CREATE INDEX ix_combined_zip_zip_county ON zip_county (combined_zip)")

        logger.info("Created zip_county index on combined_zip, creating zip_county index on dashed_zip")
        sess.execute("CREATE INDEX ix_dashed_zip_zip_county ON zip_county (dashed_zip)")

        logger.info("Created zip_county indexes, creating single_county temporary view")

        # single_county view creation
        sess.execute(
            """CREATE MATERIALIZED VIEW single_county AS (
                SELECT zip5, county_number, county_name
                FROM (SELECT
                    zip5,
                    county_number,
                    county_name,
                    ROW_NUMBER() OVER (PARTITION BY
                        zip5) AS row
                    FROM zip_county) AS tmp
                    WHERE tmp.row = 1)"""
        )

        logger.info("Created single_county temporary view, creating single_county index on zip5")

        sess.execute("CREATE INDEX ix_zip5_single_county ON single_county (zip5)")

        logger.info("Created single_county index, matview creation complete.")

    @staticmethod
    def delete_matviews(sess):
        logger.info("Deleting matviews")
        # zip_county view deletion
        sess.execute("DROP MATERIALIZED VIEW IF EXISTS single_county")
        sess.execute("DROP MATERIALIZED VIEW IF EXISTS zip_county")

        logger.info("Finished delete of matviews.")

    @staticmethod
    def update_fpds_le(sess):
        logger.info("Starting FPDS legal entity derivations, starting legal entity 9-digit zips without dashes")
        # FPDS LE 9-digit no dash
        sess.execute(
            """UPDATE transaction_fpds AS dap
                SET legal_entity_county_code = zc.county_number,
                    legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                    THEN dap.legal_entity_county_name
                                                    ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.combined_zip = dap.legal_entity_zip4
                    AND dap.legal_entity_county_code IS NULL
                    AND UPPER(dap.legal_entity_country_code) = 'USA'"""
        )

        logger.info(
            "Finished FPDS legal entity 9-digit zips without dashes, starting FPDS legal entity 9-digit zips "
            "with dashes"
        )

        # FPDS LE 9-digit dash
        sess.execute(
            """UPDATE transaction_fpds AS dap
                SET legal_entity_county_code = zc.county_number,
                    legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                    THEN dap.legal_entity_county_name
                                                    ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.dashed_zip = dap.legal_entity_zip4
                    AND dap.legal_entity_county_code IS NULL
                    AND UPPER(dap.legal_entity_country_code) = 'USA'"""
        )

        logger.info("Finished FPDS legal entity 9-digit zips with dashes, starting FPDS legal entity 5-digit zips")

        # FPDS LE 5-digit
        sess.execute(
            r"""UPDATE transaction_fpds AS dap
                SET legal_entity_county_code = sc.county_number,
                    legal_entity_county_name = CASE WHEN dap.legal_entity_county_name IS NOT NULL
                                                    THEN dap.legal_entity_county_name
                                                    ELSE sc.county_name END
                FROM single_county AS sc
                WHERE sc.zip5 = LEFT(dap.legal_entity_zip4, 5)
                    AND dap.legal_entity_county_code IS NULL
                    AND dap.legal_entity_zip4 ~ '^\d{5}(-?\d{4})?$'
                    AND UPPER(dap.legal_entity_country_code) = 'USA'"""
        )
        logger.info("Finished FPDS legal entity 5-digit zips, FPDS legal entity updates complete.")

    @staticmethod
    def update_fpds_ppop(sess):
        logger.info("Starting FPDS PPOP derivations, starting FPDS PPOP 9-digit zips without dashes")

        # FPDS PPOP 9-digit no dash
        sess.execute(
            """UPDATE transaction_fpds AS dap
                SET place_of_perform_county_co = zc.county_number,
                    place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                      THEN dap.place_of_perform_county_na
                                                      ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.combined_zip = dap.place_of_performance_zip4a
                    AND dap.place_of_perform_county_co IS NULL
                    AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
        )

        logger.info("Finished FPDS PPOP 9-digit zips without dashes, starting FPDS PPOP 9-digit zips with dashes")

        # FPDS PPOP 9-digit dash
        sess.execute(
            """UPDATE transaction_fpds AS dap
                SET place_of_perform_county_co = zc.county_number,
                    place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                      THEN dap.place_of_perform_county_na
                                                      ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.dashed_zip = dap.place_of_performance_zip4a
                    AND dap.place_of_perform_county_co IS NULL
                    AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
        )

        logger.info("Finished FPDS PPOP 9-digit zips with dashes, starting FPDS PPOP 5-digit zips")

        # FPDS PPOP 5-digit
        sess.execute(
            r"""UPDATE transaction_fpds AS dap
                SET place_of_perform_county_co = sc.county_number,
                    place_of_perform_county_na = CASE WHEN dap.place_of_perform_county_na IS NOT NULL
                                                      THEN dap.place_of_perform_county_na
                                                      ELSE sc.county_name END
                FROM single_county AS sc
                WHERE sc.zip5 = LEFT(dap.place_of_performance_zip4a, 5)
                    AND dap.place_of_perform_county_co IS NULL
                    AND dap.place_of_performance_zip4a ~ '^\d{5}(-?\d{4})?$'
                    AND UPPER(dap.place_of_perform_country_c) = 'USA'"""
        )
        logger.info("Finished FPDS PPOP 5-digit zips, FPDS PPOP updates complete")

    @staticmethod
    def update_fabs_le(sess):
        logger.info("Starting FABS legal entity derivations, starting FABS legal entity 9-digit zips")

        # FABS LE 9-digit
        sess.execute(
            """UPDATE transaction_fabs AS pafa
                SET legal_entity_county_code = zc.county_number,
                    legal_entity_county_name = CASE WHEN pafa.legal_entity_county_name IS NOT NULL
                                                      THEN pafa.legal_entity_county_name
                                                      ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.zip5 = pafa.legal_entity_zip5
                    AND zc.zip_last4 = pafa.legal_entity_zip_last4
                    AND pafa.legal_entity_county_code IS NULL
                    AND UPPER(pafa.legal_entity_country_code) = 'USA'"""
        )

        logger.info("Finished FABS legal entity 9-digit zips, starting FABS legal entity 5-digit zips")

        # FABS LE 5-digit
        sess.execute(
            """UPDATE transaction_fabs AS pafa
                SET legal_entity_county_code = sc.county_number,
                    legal_entity_county_name = CASE WHEN pafa.legal_entity_county_name IS NOT NULL
                                                      THEN pafa.legal_entity_county_name
                                                      ELSE sc.county_name END
                FROM single_county AS sc
                WHERE sc.zip5 = pafa.legal_entity_zip5
                    AND pafa.legal_entity_county_code IS NULL
                    AND UPPER(pafa.legal_entity_country_code) = 'USA'"""
        )

        logger.info("Finished FABS legal entity 5-digit zips, FABS legal entity updates complete")

    @staticmethod
    def update_fabs_ppop(sess):
        logger.info("Starting FABS PPOP derivations, starting FABS PPOP 9-digit zips without dashes")

        # FABS PPOP 9-digit no dash
        sess.execute(
            """UPDATE transaction_fabs AS pafa
                SET place_of_perform_county_co = zc.county_number,
                    place_of_perform_county_na = CASE WHEN pafa.place_of_perform_county_na IS NOT NULL
                                                      THEN pafa.place_of_perform_county_na
                                                      ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.combined_zip = pafa.place_of_performance_zip4a
                    AND pafa.place_of_perform_county_co IS NULL
                    AND UPPER(pafa.place_of_perform_country_c) = 'USA'"""
        )

        logger.info("Finished FABS PPOP 9-digit zips without dashes, starting FABS PPOP 9-digit zips with dashes")

        # FABS PPOP 9-digit dash
        sess.execute(
            """UPDATE transaction_fabs AS pafa
                SET place_of_perform_county_co = zc.county_number,
                    place_of_perform_county_na = CASE WHEN pafa.place_of_perform_county_na IS NOT NULL
                                                      THEN pafa.place_of_perform_county_na
                                                      ELSE zc.county_name END
                FROM zip_county AS zc
                WHERE zc.dashed_zip = pafa.place_of_performance_zip4a
                    AND pafa.place_of_perform_county_co IS NULL
                    AND UPPER(pafa.place_of_perform_country_c) = 'USA'"""
        )

        logger.info("Finished FABS PPOP 9-digit zips with dashes, starting FABS PPOP 5-digit zips")

        # FABS PPOP 5-digit
        sess.execute(
            r"""UPDATE transaction_fabs AS pafa
                SET place_of_perform_county_co = sc.county_number,
                    place_of_perform_county_na = CASE WHEN pafa.place_of_perform_county_na IS NOT NULL
                                                      THEN pafa.place_of_perform_county_na
                                                      ELSE sc.county_name END
                FROM single_county AS sc
                WHERE sc.zip5 = LEFT(pafa.place_of_performance_zip4a, 5)
                    AND pafa.place_of_perform_county_co IS NULL
                    AND pafa.place_of_performance_zip4a ~ '^\d{5}(-?\d{4})?$'
                    AND UPPER(pafa.place_of_perform_country_c) = 'USA'"""
        )

        logger.info("Finished FABS PPOP 5-digit zips, FABS PPOP updates complete.")

    @staticmethod
    def update_location(sess):
        logger.info("Starting location updates, starting FABS legal entity location updates")

        # Location FABS LE
        sess.execute(
            """WITH transaction_combined AS (
                SELECT
                    le.location_id,
                    tf.legal_entity_county_code AS county_code,
                    tf.legal_entity_county_name AS county_name
                FROM transaction_normalized AS tn
                JOIN legal_entity AS le
                    ON tn.recipient_id = le.legal_entity_id
                LEFT OUTER JOIN transaction_fabs AS tf
                    ON tf.transaction_id = tn.id
                WHERE UPPER(tf.legal_entity_country_code) = 'USA'
            )
            UPDATE references_location AS rl
                SET county_code = tc.county_code,
                    county_name = CASE WHEN rl.county_name IS NULL
                                        THEN tc.county_name
                                        ELSE rl.county_name END
                FROM transaction_combined AS tc
                WHERE tc.location_id = rl.location_id
                    AND rl.county_code IS NULL
                    AND UPPER(rl.location_country_code) = 'USA'"""
        )

        logger.info("Finished FABS legal entity location updates, starting FPDS legal entity location updates")

        # Location FPDS LE
        sess.execute(
            """WITH transaction_combined AS (
                SELECT
                    le.location_id,
                    tf.legal_entity_county_code AS county_code,
                    tf.legal_entity_county_name AS county_name
                FROM transaction_normalized AS tn
                JOIN legal_entity AS le
                    ON tn.recipient_id = le.legal_entity_id
                LEFT OUTER JOIN transaction_fpds AS tf
                    ON tf.transaction_id = tn.id
                WHERE UPPER(tf.legal_entity_country_code) = 'USA'
            )
            UPDATE references_location AS rl
                SET county_code = tc.county_code,
                    county_name = CASE WHEN rl.county_name IS NULL
                                        THEN tc.county_name
                                        ELSE rl.county_name END
                FROM transaction_combined AS tc
                WHERE tc.location_id = rl.location_id
                    AND rl.county_code IS NULL
                    AND UPPER(rl.location_country_code) = 'USA'"""
        )

        logger.info("Finished FPDS legal entity location updates, starting FABS PPOP location updates")

        # Location FABS PPOP
        sess.execute(
            """WITH transaction_combined AS (
                SELECT
                    tn.place_of_performance_id,
                    tf.place_of_perform_county_co AS county_code,
                    tf.place_of_perform_county_na AS county_name
                FROM transaction_normalized AS tn
                LEFT OUTER JOIN transaction_fabs AS tf
                    ON tf.transaction_id = tn.id
                WHERE UPPER(tf.place_of_perform_country_c) = 'USA'
            )
            UPDATE references_location AS rl
                SET county_code = tc.county_code,
                    county_name = CASE WHEN rl.county_name IS NULL
                                        THEN tc.county_name
                                        ELSE rl.county_name END
                FROM transaction_combined AS tc
                WHERE tc.place_of_performance_id = rl.location_id
                    AND rl.county_code IS NULL
                    AND UPPER(rl.location_country_code) = 'USA'"""
        )

        logger.info("Finished FABS PPOP location updates, starting FPDS PPOP location updates")

        # Location FPDS PPOP
        sess.execute(
            """WITH transaction_combined AS (
                SELECT
                    tn.place_of_performance_id,
                    tf.place_of_perform_county_co AS county_code,
                    tf.place_of_perform_county_na AS county_name
                FROM transaction_normalized AS tn
                LEFT OUTER JOIN transaction_fpds AS tf
                    ON tf.transaction_id = tn.id
                WHERE UPPER(tf.place_of_perform_country_c) = 'USA'
            )
            UPDATE references_location AS rl
                SET county_code = tc.county_code,
                    county_name = CASE WHEN rl.county_name IS NULL
                                        THEN tc.county_name
                                        ELSE rl.county_name END
                FROM transaction_combined AS tc
                WHERE tc.place_of_performance_id = rl.location_id
                    AND rl.county_code IS NULL
                    AND UPPER(rl.location_country_code) = 'USA'"""
        )

        logger.info("Finished FPDS PPOP location updates, finished location updates")

    def add_arguments(self, parser):
        parser.add_argument(
            "-mv", "--matview", help="Create the matviews, make sure they do not already exist", action="store_true"
        )
        parser.add_argument("-dmv", "--delete_matview", help="Delete the matviews", action="store_true")
        parser.add_argument("-fpdsle", "--fpds_le", help="Run FPDS Legal Entity updates", action="store_true")
        parser.add_argument("-fpdsppop", "--fpds_ppop", help="Run FPDS PPOP updates", action="store_true")
        parser.add_argument("-fabsle", "--fabs_le", help="Run FABS Legal Entity updates", action="store_true")
        parser.add_argument("-fabsppop", "--fabs_ppop", help="Run FABS PPOP updates", action="store_true")
        parser.add_argument("-l", "--location", help="Fix location county codes/names", action="store_true")
        parser.add_argument(
            "-a", "--all", help="Run all updates without creating or deleting matviews", action="store_true"
        )
        parser.add_argument(
            "-am", "--all_matview", help="Run all updates and create and delete matviews", action="store_true"
        )

    def handle(self, *args, **options):
        with connection.cursor() as sess:

            logger.info("Starting county code fixes")

            if options["all_matview"] or options["all"]:
                if options["all_matview"]:
                    self.create_matviews(sess)

                self.update_fpds_le(sess)
                self.update_fpds_ppop(sess)
                self.update_fabs_le(sess)
                self.update_fabs_ppop(sess)
                self.update_location(sess)

                if options["all_matview"]:
                    self.delete_matviews(sess)
            else:
                if options["matview"]:
                    self.create_matviews(sess)
                if options["fpds_le"]:
                    self.update_fpds_le(sess)
                if options["fpds_ppop"]:
                    self.update_fpds_ppop(sess)
                if options["fabs_le"]:
                    self.update_fabs_le(sess)
                if options["fabs_ppop"]:
                    self.update_fabs_ppop(sess)
                if options["location"]:
                    self.update_location(sess)
            if options["delete_matview"]:
                self.delete_matviews(sess)

            logger.info("Completed county code fixes")
