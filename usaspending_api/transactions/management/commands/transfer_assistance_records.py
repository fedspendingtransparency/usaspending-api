import logging

# from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection

# from psycopg2.sql import Composable, Composed, Identifier, SQL, Literal
# from usaspending_api.common.etl import ETLDBLinkTable, ETLTable, ETLTemporaryTable, operations, mixins
# from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.transactions.models import SourceAssistanceTransaction
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.common.retrieve_file_from_uri import SCHEMA_HELP_TEXT
from datetime import datetime, timezone


CHUNK_SIZE = 500000
SUBMISSION_LOOKBACK_MINUTES = 15


def filepath_command_line_argument_type():
    """"""

    def _filepath_command_line_argument_type(provided_uri):
        """"""
        try:
            with RetrieveFileFromUri(provided_uri).get_file_object() as file:
                # while True:
                #     lines = [int(line.decode("utf-8")) for line in file.readlines(CHUNK_SIZE)]
                #     if len(lines) == 0:
                #         break
                #     yield lines
                return [int(line.decode("utf-8")) for line in file.readlines()]

        except Exception as e:
            raise RuntimeError("Issue with reading/parsing file: {}".format(e))

    return _filepath_command_line_argument_type


class Command(BaseCommand):

    help = "Upsert assistance transactions from a broker system into USAspending"
    logger = logging.getLogger("console")
    last_load_record = "source_assistance_transaction"
    is_incremental = False

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this published_award_financial_assistance_id list (space-separated)",
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="date",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load/Reload all FPDS records from the provided datetime to the script execution start time.",
        )
        mutually_exclusive_group.add_argument(
            "--since-last-load",
            dest="incremental_date",
            action="store_true",
            help="Equivalent to loading from date, but date is drawn from last update date recorded in DB",
        )
        mutually_exclusive_group.add_argument(
            "--file",
            dest="file",
            type=filepath_command_line_argument_type(),
            help=(
                "Load/Reload transactions using published_award_financial_assistance_id values stored at this file path"
                " (one ID per line) {}".format(SCHEMA_HELP_TEXT)
            ),
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help=(
                "Script will load or reload all FABS records in broker database, from all time."
                " This does NOT clear the USASpending database first"
            ),
        )

    def handle(self, *args, **options):
        self.logger.info("starting transfer script")
        if options["incremental_date"]:
            self.logger.info("INCREMENTAL LOAD")
            self.is_incremental = True
            options["date"] = get_last_load_date(self.last_load_record, SUBMISSION_LOOKBACK_MINUTES)
            if not options["date"]:
                raise SystemExit("No date stored in the database, unable to use --since-last-load")

        self.start_time = datetime.now(timezone.utc)
        self.predicate = self.parse_options(options)

        self.logger.warn(self.predicate)
        return

        self.old_school()

        if self.is_incremental:
            update_last_load_date(self.last_load_record, self.start_time)

    def parse_options(self, options):
        """Create the SQL predicate to limit which transaction records are transfered"""
        if options["reload_all"]:
            return ""
        elif options["date"]:
            return "WHERE updated_at >= ''{}''".format(options["date"])
        else:
            ids = options["file"] if options["file"] else options["ids"]
            return "WHERE published_award_financial_assistance_id IN {}".format(tuple(ids))

    def clear_table(self):
        sql = "DELETE FROM {} WHERE true".format(SourceAssistanceTransaction().table_name)
        with connection.cursor() as cursor:
            self.logger.info("clearing {}".format(SourceAssistanceTransaction().table_name))
            cursor.execute(sql)
            self.logger.info("DELETED {} records".format(cursor.rowcount))

    def new_school(self):
        """Use Kirk's schmancy ETLTables"""

        # procurement = ETLTable(SourceProcurmentTransaction().table_name)
        # remote_procurement = ETLDBLinkTable("detached_award_procurement", "broker_server", procurement.data_types)

        # sql = SQL(UPSERT_SQL).format(
        #     destination=Identifier(SourceProcurmentTransaction().table_name),
        #     fields=Composed([Identifier(f) for f in fields]),
        #     excluded=Composable(excluded),
        #     predicate=SQL("updated_at >= '2019-10-23'")
        # )

    def old_school(self):
        fields = SourceAssistanceTransaction().model_fields
        excluded = ", ".join(["{col} = EXCLUDED.{col}".format(col=field) for field in fields])

        sql = UPSERT_SQL.format(
            destination=SourceAssistanceTransaction().table_name,
            fields=", ".join([field for field in fields]),
            excluded=excluded,
            predicate=self.predicate,
        )

        with connection.cursor() as cursor:
            with Timer(message="Upserting procurement records", success_logger=self.logger.info):
                cursor.execute(sql)
                rowcount = cursor.rowcount
                self.logger.info("Upserted {} records".format(rowcount))


UPSERT_SQL = """
INSERT INTO {destination}
({fields})
SELECT * FROM dblink (
    'broker_server',
    '
        SELECT {fields}
        FROM published_award_financial_assistance
        {predicate}
    '
    ) AS broker(
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
        published_award_financial_assistance_id integer DEFAULT nextval('published_award_financial_ass_published_award_financial_ass_seq'::regclass) PRIMARY KEY,
        action_date text,
        action_type text,
        assistance_type text,
        award_description text,
        awardee_or_recipient_legal text,
        awardee_or_recipient_uniqu text,
        awarding_agency_code text,
        awarding_office_code text,
        awarding_sub_tier_agency_c text,
        award_modification_amendme text,
        business_funds_indicator text,
        business_types text,
        cfda_number text,
        correction_delete_indicatr text,
        face_value_loan_guarantee numeric,
        fain text,
        federal_action_obligation numeric,
        fiscal_year_and_quarter_co text,
        funding_agency_code text,
        funding_office_code text,
        funding_sub_tier_agency_co text,
        legal_entity_address_line1 text,
        legal_entity_address_line2 text,
        legal_entity_address_line3 text,
        legal_entity_country_code text,
        legal_entity_foreign_city text,
        legal_entity_foreign_posta text,
        legal_entity_foreign_provi text,
        legal_entity_zip5 text,
        legal_entity_zip_last4 text,
        non_federal_funding_amount numeric,
        original_loan_subsidy_cost numeric,
        period_of_performance_curr text,
        period_of_performance_star text,
        place_of_performance_code text,
        place_of_performance_congr text,
        place_of_perform_country_c text,
        place_of_performance_forei text,
        place_of_performance_zip4a text,
        record_type integer,
        sai_number text,
        uri text,
        legal_entity_congressional text,
        total_funding_amount text,
        cfda_title text,
        awarding_agency_name text,
        awarding_sub_tier_agency_n text,
        funding_agency_name text,
        funding_sub_tier_agency_na text,
        is_historical boolean,
        place_of_perform_county_na text,
        place_of_perform_state_nam text,
        place_of_performance_city text,
        legal_entity_city_name text,
        legal_entity_county_code text,
        legal_entity_county_name text,
        legal_entity_state_code text,
        legal_entity_state_name text,
        modified_at timestamp without time zone,
        afa_generated_unique text NOT NULL,
        is_active boolean NOT NULL DEFAULT false,
        awarding_office_name text,
        funding_office_name text,
        legal_entity_city_code text,
        legal_entity_foreign_descr text,
        legal_entity_country_name text,
        place_of_perform_country_n text,
        place_of_perform_county_co text,
        submission_id numeric,
        place_of_perfor_state_code text,
        place_of_performance_zip5 text,
        place_of_perform_zip_last4 text,
        business_categories text[],
        action_type_description text,
        assistance_type_desc text,
        business_funds_ind_desc text,
        business_types_desc text,
        correction_delete_ind_desc text,
        record_type_description text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        unique_award_key text,
        high_comp_officer1_amount text,
        high_comp_officer1_full_na text,
        high_comp_officer2_amount text,
        high_comp_officer2_full_na text,
        high_comp_officer3_amount text,
        high_comp_officer3_full_na text,
        high_comp_officer4_amount text,
        high_comp_officer4_full_na text,
        high_comp_officer5_amount text,
        high_comp_officer5_full_na text
    )
)
ON CONFLICT (published_award_financial_assistance_id) DO UPDATE SET
{excluded}
RETURNING published_award_financial_assistance_id
"""
