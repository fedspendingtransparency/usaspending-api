import logging
import asyncio
import sqlparse

from django.db import connection
from django.core.management.base import BaseCommand
from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("script")

CREATE_MATVIEW_SQL = """
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_test_matview_{current_chunk} CASCADE;

CREATE MATERIALIZED VIEW universal_transaction_test_matview_{current_chunk} AS
        SELECT
            tas.treasury_account_identifiers, transaction_normalized.id AS transaction_id, transaction_normalized.action_date::date, DATE(
                transaction_normalized.action_date::date + INTERVAL '3 months'
            ) AS fiscal_action_date, transaction_normalized.last_modified_date::date, transaction_normalized.fiscal_year, awards.certified_date AS award_certified_date, FY(awards.certified_date) AS award_fiscal_year, transaction_normalized.type, transaction_normalized.award_id, transaction_normalized.update_date, awards.category AS award_category, COALESCE(CASE WHEN transaction_normalized.type IN('07', '08') THEN awards.total_subsidy_cost ELSE awards.total_obligation END, 0)::NUMERIC(
                23, 2
            ) AS award_amount, COALESCE(CASE WHEN transaction_normalized.type IN('07', '08') THEN transaction_normalized.original_loan_subsidy_cost ELSE transaction_normalized.federal_action_obligation END, 0)::NUMERIC(
                23, 2
            ) AS generated_pragmatic_obligation, awards.fain, awards.uri, awards.piid, awards.update_date AS award_update_date, awards.generated_unique_award_id, awards.type_description, awards.period_of_performance_start_date, awards.period_of_performance_current_end_date, COALESCE(transaction_normalized.federal_action_obligation, 0)::NUMERIC(
                23, 2
            ) AS federal_action_obligation, COALESCE(transaction_normalized.original_loan_subsidy_cost, 0)::NUMERIC(
                23, 2
            ) AS original_loan_subsidy_cost, COALESCE(transaction_normalized.face_value_loan_guarantee, 0)::NUMERIC(
                23, 2
            ) AS face_value_loan_guarantee, transaction_normalized.description AS transaction_description, transaction_normalized.modification_number, pop_country_lookup.country_name AS pop_country_name, pop_country_lookup.country_code AS pop_country_code, COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) AS pop_state_code, LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 3, '0') AS pop_county_code, COALESCE(pop_county_lookup.county_name, transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na) AS pop_county_name, COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5, LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 2, '0') AS pop_congressional_code, TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name, rl_country_lookup.country_code AS recipient_location_country_code, rl_country_lookup.country_name AS recipient_location_country_name, COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code, LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 3, '0') AS recipient_location_county_code, COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name, LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 2, '0') AS recipient_location_congressional_code, COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5, TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)) AS recipient_location_city_name, transaction_fpds.naics AS naics_code, naics.description AS naics_description, transaction_fpds.product_or_service_code, psc.description AS product_or_service_description, transaction_fpds.type_of_contract_pricing, transaction_fpds.type_set_aside, transaction_fpds.extent_competed, transaction_fpds.detached_award_proc_unique, transaction_fpds.ordering_period_end_date, transaction_fabs.cfda_number, transaction_fabs.afa_generated_unique, references_cfda.program_title AS cfda_title, references_cfda.id AS cfda_id, COALESCE(recipient_lookup.recipient_hash, MD5(UPPER( CASE WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu)) ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END ))::uuid) AS recipient_hash, UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name, COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AS recipient_unique_id, COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id, transaction_normalized.business_categories, transaction_normalized.awarding_agency_id, transaction_normalized.funding_agency_id, AA.toptier_agency_id AS awarding_toptier_agency_id, FA.toptier_agency_id AS funding_toptier_agency_id, TAA.name AS awarding_toptier_agency_name, TFA.name AS funding_toptier_agency_name, SAA.name AS awarding_subtier_agency_name, SFA.name AS funding_subtier_agency_name, TAA.abbreviation AS awarding_toptier_agency_abbreviation, TFA.abbreviation AS funding_toptier_agency_abbreviation, SAA.abbreviation AS awarding_subtier_agency_abbreviation, SFA.abbreviation AS funding_subtier_agency_abbreviation
        FROM
            transaction_normalized
        LEFT OUTER JOIN transaction_fabs ON
            (
                transaction_normalized.id = transaction_fabs.transaction_id
                AND transaction_normalized.is_fpds = FALSE
            )
        LEFT OUTER JOIN transaction_fpds ON
            (
                transaction_normalized.id = transaction_fpds.transaction_id
                AND transaction_normalized.is_fpds = TRUE
            )
        LEFT OUTER JOIN references_cfda ON
            (
                transaction_fabs.cfda_number = references_cfda.program_number
            )
        LEFT OUTER JOIN (
                SELECT
                    recipient_hash, legal_business_name AS recipient_name, duns
                FROM
                    recipient_lookup AS rlv
            ) recipient_lookup ON
            recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu)
            AND COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
        LEFT OUTER JOIN awards ON
            (
                transaction_normalized.award_id = awards.id
            )
        LEFT OUTER JOIN agency AS AA ON
            (
                transaction_normalized.awarding_agency_id = AA.id
            )
        LEFT OUTER JOIN toptier_agency AS TAA ON
            (
                AA.toptier_agency_id = TAA.toptier_agency_id
            )
        LEFT OUTER JOIN subtier_agency AS SAA ON
            (
                AA.subtier_agency_id = SAA.subtier_agency_id
            )
        LEFT OUTER JOIN agency AS FA ON
            (
                transaction_normalized.funding_agency_id = FA.id
            )
        LEFT OUTER JOIN toptier_agency AS TFA ON
            (
                FA.toptier_agency_id = TFA.toptier_agency_id
            )
        LEFT OUTER JOIN subtier_agency AS SFA ON
            (
                FA.subtier_agency_id = SFA.subtier_agency_id
            )
        LEFT OUTER JOIN naics ON
            (
                transaction_fpds.naics = naics.code
            )
        LEFT OUTER JOIN psc ON
            (
                transaction_fpds.product_or_service_code = psc.code
            )
        LEFT OUTER JOIN (
                SELECT
                    faba.award_id, ARRAY_AGG(DISTINCT taa.treasury_account_identifier) treasury_account_identifiers
                FROM
                    treasury_appropriation_account taa
                INNER JOIN financial_accounts_by_awards faba ON
                    taa.treasury_account_identifier = faba.treasury_account_id
                WHERE
                    faba.award_id IS NOT NULL
                GROUP BY
                    faba.award_id
            ) tas ON
            (
                tas.award_id = transaction_normalized.award_id
            )
        LEFT OUTER JOIN (
                SELECT
                    DISTINCT ON
                    (
                        state_alpha, county_numeric
                    ) state_alpha, county_numeric, UPPER(county_name) AS county_name
                FROM
                    ref_city_county_state_code
            ) AS rl_county_lookup ON
            rl_county_lookup.state_alpha = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
            AND rl_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 3, '0')
        LEFT OUTER JOIN (
                SELECT
                    DISTINCT ON
                    (
                        state_alpha, county_numeric
                    ) state_alpha, county_numeric, UPPER(county_name) AS county_name
                FROM
                    ref_city_county_state_code
            ) AS pop_county_lookup ON
            pop_county_lookup.state_alpha = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
            AND pop_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS SMALLINT) AS TEXT), 3, '0')
        LEFT OUTER JOIN ref_country_code AS pop_country_lookup ON
            (
                pop_country_lookup.country_code = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA')
                OR pop_country_lookup.country_name = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c)
            )
        LEFT OUTER JOIN ref_country_code AS rl_country_lookup ON
            (
                rl_country_lookup.country_code = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA')
                OR rl_country_lookup.country_name = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code)
            )
        WHERE
            transaction_normalized.action_date >= '2000-10-01'
            AND transaction_normalized.id % {thread_count} = {current_chunk}
    ;
"""

REFRESH_MATVIEW_SQL = """
REFRESH MATERIALIZED VIEW universal_transaction_test_matview_{current_chunk};
"""

RECREATE_TABLE_SQL = """
DROP TABLE IF EXISTS universal_transaction_test_table;
CREATE TABLE universal_transaction_test_table AS TABLE universal_transaction_matview WITH NO DATA;
"""

INSERT_INTO_TABLE_SQL = """
INSERT INTO universal_transaction_test_table SELECT * FROM universal_transaction_test_matview_{current_chunk}
"""

TABLE_INDEX_SQL = """
CREATE UNIQUE INDEX idx_15d6e514$75d_transaction_id_test_table ON universal_transaction_test_table USING BTREE(transaction_id ASC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_15d6e514$75d_action_date_test_table ON universal_transaction_test_table USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_last_modified_date_test_table ON universal_transaction_test_table USING BTREE(last_modified_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_15d6e514$75d_fiscal_year_test_table ON universal_transaction_test_table USING BTREE(fiscal_year DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_type_test_table ON universal_transaction_test_table USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_award_id_test_table ON universal_transaction_test_table USING BTREE(award_id) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_pop_zip5_test_table ON universal_transaction_test_table USING BTREE(pop_zip5) WITH (fillfactor = 97) WHERE pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_recipient_unique_id_test_table ON universal_transaction_test_table USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_parent_recipient_unique_id_test_table ON universal_transaction_test_table USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97) WHERE parent_recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_simple_pop_geolocation_test_table ON universal_transaction_test_table USING BTREE(pop_state_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_recipient_hash_test_table ON universal_transaction_test_table USING BTREE(recipient_hash) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_15d6e514$75d_action_date_pre2008_test_table ON universal_transaction_test_table USING BTREE(action_date) WITH (fillfactor = 97) WHERE action_date < '2007-10-01';
"""


class Command(BaseCommand):

    help = "" ""

    def add_arguments(self, parser):
        parser.add_argument("--thread-count", default=10, help="Broker submission_id to load", type=int)
        parser.add_argument("--refresh", action="store_true", help="Refreshes Matview")
        parser.add_argument("--recreate", action="store_true", help="Recreates Matview")
        parser.add_argument("--update-table", action="store_true", help="Truncates the aggregate table and inserts data from all matviews")

    def handle(self, *args, **options):
        thread_count = options["thread_count"]
        refresh = options["refresh"]
        recreate = options["recreate"]
        update_table = options["update_table"]

        logger.info("Thread Count: {}".format(thread_count))

        if recreate:
            with Timer("Creating Matviews"):
                self.create_matviews(thread_count)
        if refresh:
            with Timer("Refreshing Matviews"):
                self.refresh_matviews(thread_count)

        if update_table:
            with Timer("Recreating table"):
                self.recreate_table()
            with Timer("Inserting data into table"):
                self.insert_table_data(thread_count)
            with Timer("Creating table indexes"):
                self.create_indexes()

    def create_matviews(self, thread_count):
        loop = asyncio.new_event_loop()
        tasks = []
        for current_chunk in range(0, thread_count):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        CREATE_MATVIEW_SQL.format(thread_count=thread_count, current_chunk=current_chunk),
                        wrapper=Timer("Create Matview {}".format(current_chunk)),
                    ),
                    loop=loop,
                )
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    def refresh_matviews(self, thread_count):
        loop = asyncio.new_event_loop()
        tasks = []
        for current_chunk in range(0, thread_count):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        REFRESH_MATVIEW_SQL.format(current_chunk=current_chunk),
                        wrapper=Timer("Refresh Matview {}".format(current_chunk)),
                    ),
                    loop=loop,
                )
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    def recreate_table(self):
        with connection.cursor() as cursor:
            cursor.execute(RECREATE_TABLE_SQL)

    def insert_table_data(self, thread_count):
        loop = asyncio.new_event_loop()
        tasks = []
        for current_chunk in range(0, thread_count):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        INSERT_INTO_TABLE_SQL.format(current_chunk=current_chunk),
                        wrapper=Timer("Insert into table {}".format(current_chunk)),
                    ),
                    loop=loop,
                )
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    def create_indexes(self):
        loop = asyncio.new_event_loop()
        tasks = []
        i = 0
        for sql in sqlparse.split(TABLE_INDEX_SQL):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        sql,
                        wrapper=Timer("Creating Index {}".format(i)),
                    ),
                    loop=loop,
                )
            )
            i += 1

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
