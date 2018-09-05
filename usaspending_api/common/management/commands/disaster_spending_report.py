import logging
from django.core.management.base import BaseCommand
from openpyxl import load_workbook
import json
import itertools
import os

TAS_XLSX_FILE = 'usaspending_api/data/DEFC ABC Pd 6 FY18.xlsx'


class Command(BaseCommand):
    """
    """
    help = "Generate CSV for specific TAS"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument(
            "-d",
            "--destination",
            default=os.path.dirname(os.path.abspath(__file__)),
            type=str,
            help='Set for a custom location of output files')

    def handle(self, *args, **options):
        self.contract_results = []
        self.assistance_results = []
        self.destination = os.path.dirname(options["destination"])
        tas_dict_list = self.gather_tas_from_file()
        self.query_database(tas_dict_list)
        self.compile_results_to_csv()

    def gather_tas_from_file(self):
        wb = load_workbook(filename=TAS_XLSX_FILE, read_only=True)
        ws = wb["DEFC"]
        ws.calculate_dimension()
        tas_code_header = ws["A8":"G8"]
        expected_headers = ["ATA", "AID", "BPOA", "EPOA", "AvailType Code", "Main", "Sub"]
        headers = []
        for header in tas_code_header:
            for cell in header:
                headers.append(cell.value.replace('\n', '').strip())
        if expected_headers != headers:
            raise Exception("Headers {} Don't match expected: {}".format(headers, expected_headers))

        tas_code_rows = ws["A9":"G100"]
        # Turn "rows" of "cell" into a list of dictionaries using the headers. I'm sure pandas could have done it too
        tas_dicts = [
            {
                key: cell.value.strip() if cell.value else None for key, cell in itertools.zip_longest(headers, row)
            } for row in tas_code_rows
        ]

        for tas in tas_dicts:
            print(json.dumps(tas))
        return tas_dicts

    def query_database(self, tas_dict_list):
        for tas in tas_dict_list:
            self.contract_results.append(self.single_tas_query(tas, 'contract'))
            self.assistance_results.append(self.single_tas_query(tas, 'assistance'))
            break

    def single_tas_query(self, tas_dict, transaction_type='contract'):
        if transaction_type == "contract":
            sql_string = CONTRACT_SQL
        else:
            sql_string = ASSISTANCE_SQL
        formatted_dict = {k: "= '{}'".format(v) if v else 'IS NULL' for k, v in tas_dict.items()}
        print(sql_string.format(**formatted_dict))
        # DO SQL call HERE
        # return DB rows
        return []

    def compile_results_to_csv(self):
        pass


ASSISTANCE_SQL = ''''''

CONTRACT_SQL = '''
SELECT
    financial_accounts_by_awards.reporting_period_start,
    financial_accounts_by_awards.reporting_period_end,
    "financial_accounts_by_awards"."reporting_period_end" AS "submission_period",
    "treasury_appropriation_account"."allocation_transfer_agency_id" AS "allocation_transfer_agency_identifier",
    "treasury_appropriation_account"."agency_id" AS "agency_identifer",
    "treasury_appropriation_account"."beginning_period_of_availability" AS "beginning_period_of_availability",
    "treasury_appropriation_account"."ending_period_of_availability" AS "ending_period_of_availability",
    "treasury_appropriation_account"."availability_type_code" AS "availability_type_code",
    "treasury_appropriation_account"."main_account_code" AS "main_account_code",
    "treasury_appropriation_account"."sub_account_code" AS "sub_account_code",

    CONCAT("treasury_appropriation_account"."agency_id",
        CONCAT('-',
        CONCAT(CASE
            WHEN "treasury_appropriation_account"."availability_type_code" = 'X' THEN 'X'
            ELSE CONCAT("treasury_appropriation_account"."beginning_period_of_availability",
            CONCAT('/' ,
            "treasury_appropriation_account"."ending_period_of_availability"))
        END,
        CONCAT('-',
        CONCAT("treasury_appropriation_account"."main_account_code",
        CONCAT('-',
        "treasury_appropriation_account"."sub_account_code")))))) AS "treasury_account_symbol",

    (SELECT U0."name" FROM "toptier_agency" U0 WHERE U0."cgac_code" = ( "treasury_appropriation_account"."agency_id") LIMIT 1)
      AS "agency_name",
    (SELECT U0. "name" FROM "toptier_agency" U0 WHERE U0."cgac_code" = ( "treasury_appropriation_account"."allocation_transfer_agency_id") LIMIT 1)
      AS "allocation_transfer_agency_name",
    "treasury_appropriation_account"."budget_function_title" AS "budget_function",
    "treasury_appropriation_account"."budget_subfunction_title" AS "budget_subfunction",
    CONCAT ("federal_account"."agency_identifier", CONCAT ('-', "federal_account"."main_account_code"))
      AS "federal_account_symbol",
    "federal_account"."account_title" AS "federal_account_name",
    "ref_program_activity"."program_activity_code" AS "program_activity_code",
    "ref_program_activity"."program_activity_name" AS "program_activity_name",
    "object_class"."object_class" AS "object_class_code",
    "object_class"."object_class_name" AS "object_class_name",
    "object_class"."direct_reimbursable" AS "direct_or_reimbursable_funding_source",
    "financial_accounts_by_awards"."piid" AS "piid",
    "financial_accounts_by_awards"."parent_award_id" AS "parent_award_piid",
    "financial_accounts_by_awards"."fain" AS "fain",
    "financial_accounts_by_awards"."uri" AS "uri",


    financial_accounts_by_awards.obligations_incurred_total_by_award_cpe,
    financial_accounts_by_awards.gross_outlay_amount_by_award_cpe,
    financial_accounts_by_awards.gross_outlay_amount_by_award_fyb,

    transaction_fpds.awarding_office_code AS awarding_office_code,
    transaction_fpds.awarding_office_name AS awarding_office_name,
    transaction_fpds.funding_office_code AS funding_office_code,
    transaction_fpds.funding_office_name AS funding_office_name,
    transaction_fpds.funding_sub_tier_agency_co AS funding_sub_tier_agency_code,
    transaction_fpds.funding_sub_tier_agency_na AS funding_sub_tier_agency_name,
    transaction_fpds.funding_agency_code AS funding_agency_code,
    transaction_fpds.funding_agency_name AS funding_agency_name,
    transaction_fpds.federal_action_obligation AS federal_action_obligation,
    awards.total_obligation AS total_dollars_obligated,
    transaction_fpds.current_total_value_award,
    awards.period_of_performance_start_date,
    awards.period_of_performance_current_end_date,
    transaction_fpds.national_interest_action,
    transaction_fpds.national_interest_desc AS national_interest_description_tag,

    "financial_accounts_by_awards"."transaction_obligated_amount" AS "transaction_obligated_amount",
    "awards"."type" AS "award_type_code",
    "awards"."type_description" AS "award_type",
    "transaction_fpds"."idv_type" AS "idv_type_code",
    "transaction_fpds"."idv_type_description" AS "idv_type",
    "awards"."description" AS "award_description",
    "toptier_agency"."cgac_code" AS "awarding_agency_code",
    "toptier_agency"."name" AS "awarding_agency_name",
    "subtier_agency"."subtier_code" AS "awarding_subagency_code",
    "subtier_agency"."name" AS "awarding_subagency_name",
    "legal_entity"."recipient_unique_id" AS "recipient_duns",
    "legal_entity"."recipient_name" AS "recipient_name",
    "legal_entity"."parent_recipient_unique_id" AS "recipient_parent_duns",
    "transaction_fpds"."ultimate_parent_legal_enti" AS "recipient_parent_name",
    "references_location"."country_name" AS "recipient_country",
    "references_location"."state_name" AS "recipient_state",
    "references_location"."county_name" AS "recipient_county",
    "references_location"."city_name" AS "recipient_city",
    "references_location"."congressional_code" AS "recipient_congressional_district",
    "references_location"."zip4" AS "recipient_zip_code",
    pop_location."country_name" AS "primary_place_of_performance_country",
    pop_location."state_name" AS "primary_place_of_performance_state",
    pop_location."county_name" AS "primary_place_of_performance_county",
    pop_location."congressional_code" AS "primary_place_of_performance_congressional_district",
    pop_location."zip4" AS "primary_place_of_performance_zip_4",
    "transaction_fpds"."naics" AS "naics_code",
    "transaction_fpds"."naics_description" AS "naics_description"
    FROM
    "financial_accounts_by_awards"
    INNER JOIN
    "treasury_appropriation_account"
        ON (
            "financial_accounts_by_awards"."treasury_account_id" = "treasury_appropriation_account"."treasury_account_identifier"
       )
    LEFT OUTER JOIN
    "federal_account"
        ON (
            "treasury_appropriation_account"."federal_account_id" = "federal_account"."id"
       )
    LEFT OUTER JOIN
    "awards"
        ON (
            "financial_accounts_by_awards"."award_id" = "awards"."id"
       )
    LEFT OUTER JOIN
    "transaction_normalized"
        ON (
            "awards"."latest_transaction_id" = "transaction_normalized"."id"
       )
    LEFT OUTER JOIN
    "transaction_fpds"
        ON (
            "transaction_normalized"."id" = "transaction_fpds"."transaction_id"
       )
    LEFT OUTER JOIN
    "ref_program_activity"
        ON (
            "financial_accounts_by_awards"."program_activity_id" = "ref_program_activity"."id"
       )
    LEFT OUTER JOIN
    "object_class"
        ON (
            "financial_accounts_by_awards"."object_class_id" = "object_class"."id"
       )
    LEFT OUTER JOIN
    "agency"
        ON (
            "awards"."awarding_agency_id" = "agency"."id"
       )
    LEFT OUTER JOIN
    "toptier_agency"
        ON (
            "agency"."toptier_agency_id" = "toptier_agency"."toptier_agency_id"
       )
    LEFT OUTER JOIN
    "subtier_agency"
        ON (
            "agency"."subtier_agency_id" = "subtier_agency"."subtier_agency_id"
       )
    LEFT OUTER JOIN
    "legal_entity"
        ON (
            "awards"."recipient_id" = "legal_entity"."legal_entity_id"
       )
    LEFT OUTER JOIN
    "references_location"
        ON (
            "legal_entity"."location_id" = "references_location"."location_id"
       )
    LEFT OUTER JOIN
    "references_location" pop_location
        ON (
            "awards"."place_of_performance_id" = pop_location.location_id
       )
    WHERE
    (
        (
            treasury_appropriation_account.allocation_transfer_agency_id {ATA} AND
            treasury_appropriation_account.agency_id {AID} AND
            treasury_appropriation_account.availability_type_code {AvailType Code} AND
            treasury_appropriation_account.beginning_period_of_availability {BPOA} AND
            treasury_appropriation_account.ending_period_of_availability {EPOA} AND
            treasury_appropriation_account.main_account_code {Main} AND
            treasury_appropriation_account.sub_account_code {Sub}
        )
        AND "transaction_normalized"."type" IN ('B','C','D','A'));'''
