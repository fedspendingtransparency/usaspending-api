import logging

# from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection

# from psycopg2.sql import Composable, Composed, Identifier, SQL, Literal
# from usaspending_api.common.etl import ETLDBLinkTable, ETLTable, ETLTemporaryTable, operations, mixins
# from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.transactions.models import SourceProcurmentTransaction
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.common.retrieve_file_from_uri import SCHEMA_HELP_TEXT
from datetime import datetime, timezone


CHUNK_SIZE = 500000


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

    help = "Upsert procurement transactions from a broker system into USAspending"
    logger = logging.getLogger("console")
    last_load_record = "source_procurement_transaction"
    is_incremental = False

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this detached_award_procurement_id list (space-separated)",
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
                "Load/Reload transactions using detached_award_procurement_id values stored at this file path"
                " (one ID per line) {}".format(SCHEMA_HELP_TEXT)
            ),
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help=(
                "Script will load or reload all FPDS records in broker database, from all time. "
                "This does NOT clear the USASpending database first"
            ),
        )

    def handle(self, *args, **options):
        self.logger.info("starting transfer script")
        if options["incremental_date"]:
            self.logger.info("INCREMENTAL LOAD")
            self.is_incremental = True
            options["date"] = get_last_load_date(self.last_load_record)
            if not options["date"]:
                raise SystemExit("No date stored in the database, unable to use --since-last-load")

        self.start_time = datetime.now(timezone.utc)
        self.predicate = self.parse_options(options)

        self.logger.warn(self.predicate)
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
            return "WHERE detached_award_procurement_id IN {}".format(tuple(ids))

    def clear_table(self):
        sql = "DELETE FROM {} WHERE true".format(SourceProcurmentTransaction().table_name)
        with connection.cursor() as cursor:
            self.logger.info("clearing {}".format(SourceProcurmentTransaction().table_name))
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
        fields = SourceProcurmentTransaction().model_fields
        excluded = ", ".join(["{col} = EXCLUDED.{col}".format(col=field) for field in fields])

        sql = UPSERT_SQL.format(
            destination=SourceProcurmentTransaction().table_name,
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
        FROM detached_award_procurement
        {predicate}
    '
    ) AS broker (
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
        detached_award_procurement_id integer,
        detached_award_proc_unique text,
        a_76_fair_act_action text,
        a_76_fair_act_action_desc text,
        action_date text,
        action_type text,
        action_type_description text,
        additional_reporting text,
        agency_id text,
        airport_authority text,
        alaskan_native_owned_corpo text,
        alaskan_native_servicing_i text,
        american_indian_owned_busi text,
        annual_revenue text,
        asian_pacific_american_own text,
        award_description text,
        award_modification_amendme text,
        award_or_idv_flag text,
        awardee_or_recipient_legal text,
        awardee_or_recipient_uniqu text,
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_office_code text,
        awarding_office_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        base_and_all_options_value text,
        base_exercised_options_val text,
        black_american_owned_busin text,
        business_categories text[],
        c1862_land_grant_college text,
        c1890_land_grant_college text,
        c1994_land_grant_college text,
        c8a_program_participant text,
        cage_code text,
        city_local_government text,
        clinger_cohen_act_pla_desc text,
        clinger_cohen_act_planning text,
        commercial_item_acqui_desc text,
        commercial_item_acquisitio text,
        commercial_item_test_desc text,
        commercial_item_test_progr text,
        community_developed_corpor text,
        community_development_corp text,
        consolidated_contract text,
        consolidated_contract_desc text,
        construction_wage_rat_desc text,
        construction_wage_rate_req text,
        contingency_humanitar_desc text,
        contingency_humanitarian_o text,
        contract_award_type text,
        contract_award_type_desc text,
        contract_bundling text,
        contract_bundling_descrip text,
        contract_financing text,
        contract_financing_descrip text,
        contracting_officers_desc text,
        contracting_officers_deter text,
        contracts text,
        corporate_entity_not_tax_e text,
        corporate_entity_tax_exemp text,
        cost_accounting_stand_desc text,
        cost_accounting_standards text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        council_of_governments text,
        country_of_product_or_desc text,
        country_of_product_or_serv text,
        county_local_government text,
        current_total_value_award text,
        division_name text,
        division_number_or_office text,
        dod_claimant_prog_cod_desc text,
        dod_claimant_program_code text,
        domestic_or_foreign_e_desc text,
        domestic_or_foreign_entity text,
        domestic_shelter text,
        dot_certified_disadvantage text,
        economically_disadvantaged text,
        educational_institution text,
        emerging_small_business text,
        epa_designated_produc_desc text,
        epa_designated_product text,
        evaluated_preference text,
        evaluated_preference_desc text,
        extent_compete_description text,
        extent_competed text,
        fair_opportunity_limi_desc text,
        fair_opportunity_limited_s text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        federal_action_obligation numeric,
        federal_agency text,
        federally_funded_research text,
        for_profit_organization text,
        foreign_funding text,
        foreign_funding_desc text,
        foreign_government text,
        foreign_owned_and_located text,
        foundation text,
        funding_agency_code text,
        funding_agency_name text,
        funding_office_code text,
        funding_office_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        government_furnished_desc text,
        government_furnished_prope text,
        grants text,
        high_comp_officer1_amount text,
        high_comp_officer1_full_na text,
        high_comp_officer2_amount text,
        high_comp_officer2_full_na text,
        high_comp_officer3_amount text,
        high_comp_officer3_full_na text,
        high_comp_officer4_amount text,
        high_comp_officer4_full_na text,
        high_comp_officer5_amount text,
        high_comp_officer5_full_na text,
        hispanic_american_owned_bu text,
        hispanic_servicing_institu text,
        historically_black_college text,
        historically_underutilized text,
        hospital_flag text,
        housing_authorities_public text,
        idv_type text,
        idv_type_description text,
        indian_tribe_federally_rec text,
        information_technolog_desc text,
        information_technology_com text,
        inherently_government_desc text,
        inherently_government_func text,
        initial_report_date text,
        inter_municipal_local_gove text,
        interagency_contract_desc text,
        interagency_contracting_au text,
        international_organization text,
        interstate_entity text,
        joint_venture_economically text,
        joint_venture_women_owned text,
        labor_standards text,
        labor_standards_descrip text,
        labor_surplus_area_firm text,
        last_modified text,
        legal_entity_address_line1 text,
        legal_entity_address_line2 text,
        legal_entity_address_line3 text,
        legal_entity_city_name text,
        legal_entity_congressional text,
        legal_entity_country_code text,
        legal_entity_country_name text,
        legal_entity_county_code text,
        legal_entity_county_name text,
        legal_entity_state_code text,
        legal_entity_state_descrip text,
        legal_entity_zip4 text,
        legal_entity_zip5 text,
        legal_entity_zip_last4 text,
        limited_liability_corporat text,
        local_area_set_aside text,
        local_area_set_aside_desc text,
        local_government_owned text,
        major_program text,
        manufacturer_of_goods text,
        materials_supplies_article text,
        materials_supplies_descrip text,
        minority_institution text,
        minority_owned_business text,
        multi_year_contract text,
        multi_year_contract_desc text,
        multiple_or_single_aw_desc text,
        multiple_or_single_award_i text,
        municipality_local_governm text,
        naics text,
        naics_description text,
        national_interest_action text,
        national_interest_desc text,
        native_american_owned_busi text,
        native_hawaiian_owned_busi text,
        native_hawaiian_servicing text,
        nonprofit_organization text,
        number_of_actions text,
        number_of_employees text,
        number_of_offers_received text,
        ordering_period_end_date text,
        organizational_type text,
        other_minority_owned_busin text,
        other_not_for_profit_organ text,
        other_statutory_authority text,
        other_than_full_and_o_desc text,
        other_than_full_and_open_c text,
        parent_award_id text,
        partnership_or_limited_lia text,
        performance_based_se_desc text,
        performance_based_service text,
        period_of_perf_potential_e text,
        period_of_performance_curr text,
        period_of_performance_star text,
        piid text,
        place_of_manufacture text,
        place_of_manufacture_desc text,
        place_of_perf_country_desc text,
        place_of_perfor_state_desc text,
        place_of_perform_city_name text,
        place_of_perform_country_c text,
        place_of_perform_country_n text,
        place_of_perform_county_co text,
        place_of_perform_county_na text,
        place_of_perform_state_nam text,
        place_of_perform_zip_last4 text,
        place_of_performance_congr text,
        place_of_performance_locat text,
        place_of_performance_state text,
        place_of_performance_zip4a text,
        place_of_performance_zip5 text,
        planning_commission text,
        port_authority text,
        potential_total_value_awar text,
        price_evaluation_adjustmen text,
        private_university_or_coll text,
        product_or_service_co_desc text,
        product_or_service_code text,
        program_acronym text,
        program_system_or_equ_desc text,
        program_system_or_equipmen text,
        pulled_from text,
        purchase_card_as_paym_desc text,
        purchase_card_as_payment_m text,
        receives_contracts_and_gra text,
        recovered_materials_s_desc text,
        recovered_materials_sustai text,
        referenced_idv_agency_desc text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_name text,
        referenced_idv_modificatio text,
        referenced_idv_type text,
        referenced_idv_type_desc text,
        referenced_mult_or_si_desc text,
        referenced_mult_or_single text,
        research text,
        research_description text,
        sam_exception text,
        sam_exception_description text,
        sba_certified_8_a_joint_ve text,
        school_district_local_gove text,
        school_of_forestry text,
        sea_transportation text,
        sea_transportation_desc text,
        self_certified_small_disad text,
        service_disabled_veteran_o text,
        small_agricultural_coopera text,
        small_business_competitive text,
        small_disadvantaged_busine text,
        sole_proprietorship text,
        solicitation_date text,
        solicitation_identifier text,
        solicitation_procedur_desc text,
        solicitation_procedures text,
        state_controlled_instituti text,
        subchapter_s_corporation text,
        subcontinent_asian_asian_i text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        the_ability_one_program text,
        total_obligated_amount text,
        township_local_government text,
        transaction_number text,
        transit_authority text,
        tribal_college text,
        tribally_owned_business text,
        type_of_contract_pric_desc text,
        type_of_contract_pricing text,
        type_of_idc text,
        type_of_idc_description text,
        type_set_aside text,
        type_set_aside_description text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        undefinitized_action text,
        undefinitized_action_desc text,
        unique_award_key text,
        us_federal_government text,
        us_government_entity text,
        us_local_government text,
        us_state_government text,
        us_tribal_government text,
        vendor_alternate_name text,
        vendor_alternate_site_code text,
        vendor_doing_as_business_n text,
        vendor_enabled text,
        vendor_fax_number text,
        vendor_legal_org_name text,
        vendor_location_disabled_f text,
        vendor_phone_number text,
        vendor_site_code text,
        veteran_owned_business text,
        veterinary_college text,
        veterinary_hospital text,
        woman_owned_business text,
        women_owned_small_business text
)
ON CONFLICT (detached_award_procurement_id) DO UPDATE SET
{excluded}
RETURNING detached_award_procurement_id
"""
