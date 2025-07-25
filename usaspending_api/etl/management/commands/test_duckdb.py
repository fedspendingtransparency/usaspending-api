import math
import os
import time

import duckdb
import psutil
from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG

SPARK_S3_BUCKET = CONFIG.SPARK_S3_BUCKET
DELTA_LAKE_S3_PATH = CONFIG.DELTA_LAKE_S3_PATH
S3_DELTA_PATH = f"s3://{SPARK_S3_BUCKET}/{DELTA_LAKE_S3_PATH}/rpt/award_search"

DOWNLOAD_S3_BUCKET = CONFIG.BULK_DOWNLOAD_S3_BUCKET_NAME
S3_DOWNLOAD_PATH = f"s3://{DOWNLOAD_S3_BUCKET}"

DUCKDB_EXTENSIONS = ["delta", "httpfs", "aws", "postgres"]


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--is-local", action="store_true")

    def handle(self, *args, **options):
        # Read arguments
        is_local = options["is_local"]

        # Establish DuckDB connection and install plugins
        conn = duckdb.connect()

        # Try to load the required extensions and install them only if they are missing
        for extension in DUCKDB_EXTENSIONS:
            try:
                conn.load_extension(extension)
            except duckdb.IOException:
                try:
                    print(f"Failed to load extension '{extension}', so trying to install it.")
                    conn.install_extension(extension)
                    print(f"Successfully installed: {extension}. Trying to load it again.\n")
                    conn.load_extension(extension)
                except duckdb.IOException:
                    print(f"Failed to install extension: {extension}. Exiting.")
                    exit(1)
            print(f"Extension successfully loaded: {extension}")

        if is_local:
            minio_host = os.getenv("MINIO_HOST", "minio")
            minio_port = os.getenv("MINIO_PORT", "10001")
            minio_user = os.getenv("MINIO_ROOT_USER", "usaspending")
            minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "usaspender")

            conn.execute(f"""
            CREATE SECRET delta_secret (
                TYPE s3,
                KEY_ID '{minio_user}',
                SECRET '{minio_pass}',
                REGION 'us-east-1',
                ENDPOINT '{minio_host}:{minio_port}',
                URL_STYLE 'path',
                USE_SSL 'false'
            );
            """)
        else:
            conn.execute("""
            CREATE SECRET delta_secret (
                TYPE s3,
                REGION 'us-gov-west-1',
                PROVIDER 'credential_chain',
                ENDPOINT 's3.us-gov-west-1.amazonaws.com'
            );
            """)

        # conn.execute(f"""
        # CREATE SECRET postgres_secret (
        #     TYPE postgres,
        #     HOST {os.getenv("USASPENDING_DB_HOST", "localhost")},
        #     PORT 5432,
        #     DATABASE {os.getenv("USASPENDING_DB_NAME", "data_store_api")},
        #     USER {os.getenv("USASPENDING_DB_USER", "usaspending")},
        #     PASSWORD {os.getenv("USASPENDING_DB_PASSWORD", "usaspender")}
        # );
        # """)

        # query = conn.from_query(f"FROM delta_scan('{S3_DELTA_PATH}');").select("*").order("award_amount desc")
        # print(f"Attempting to read from S3 Location: {S3_DELTA_PATH}")
        # print("SQL results:")
        # print(query)

        ###################
        # Memory settings #
        ###################
        print()
        print("#" * 50)
        available_memory = math.floor(psutil.virtual_memory().available / (1024**3))
        print(f"OS available memory:\t\t{available_memory} GB")

        safe_memory_limit = int(psutil.virtual_memory().available / (1024**3) * 0.8)
        print(f"Manual DuckDB memory limit:\t{safe_memory_limit} GB")

        # Manually set the memory limit to 80%, because DuckDB sees the amount of RAM on the EC2 instance (1,300 PB)
        #   instead of the memory available in the container
        conn.execute(f"SET memory_limit = '{safe_memory_limit}GB';")
        duckdb_memory_limit = conn.sql("SELECT current_setting('memory_limit') AS memory_limit;").to_df()[
            "memory_limit"
        ][0]
        print(f"DuckDB memory limit:\t\t{duckdb_memory_limit}")

        duckdb_threads_limit = conn.sql("SELECT current_setting('threads') AS threads;").to_df()["threads"][0]
        print(f"DuckDB threads limit:\t\t{duckdb_threads_limit}")
        print("#" * 50)

        conn.execute("SET enable_progress_bar = true;")

        filename = "ta_duckdb_pg.csv"
        conn.execute(f"ATTACH '{os.getenv('DATABASE_URL')}' AS usas (TYPE postgres, READ_ONLY);")

        ##########
        # File A #
        ##########
        print(f"\nStarting available memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
        print("Generating File A download")
        start_time = time.perf_counter()
        file_a_ta_query = """
        SELECT
            toptier_agency.name AS owning_agency_name,
            submission_attributes.reporting_agency_name AS reporting_agency_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('Q') :: text,
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        ) :: varchar
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
                ELSE CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('P') :: text,
                                    (
                                        LPAD(
                                            (
                                                submission_attributes.reporting_fiscal_period
                                            ) :: varchar,
                                            2,
                                            '0'
                                        )
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
            END AS submission_period,
            treasury_appropriation_account.allocation_transfer_agency_id AS allocation_transfer_agency_identifier_code,
            treasury_appropriation_account.agency_id AS agency_identifier_code,
            treasury_appropriation_account.beginning_period_of_availability AS beginning_period_of_availability,
            treasury_appropriation_account.ending_period_of_availability AS ending_period_of_availability,
            treasury_appropriation_account.availability_type_code AS availability_type_code,
            treasury_appropriation_account.main_account_code AS main_account_code,
            treasury_appropriation_account.sub_account_code AS sub_account_code,
            treasury_appropriation_account.tas_rendering_label AS treasury_account_symbol,
            treasury_appropriation_account.account_title AS treasury_account_name,
            vw_appropriation_account_balances_download.agency_identifier_name AS agency_identifier_name,
            vw_appropriation_account_balances_download.allocation_transfer_agency_identifier_name AS allocation_transfer_agency_identifier_name,
            treasury_appropriation_account.budget_function_title AS budget_function,
            treasury_appropriation_account.budget_subfunction_title AS budget_subfunction,
            federal_account.federal_account_code AS federal_account_symbol,
            federal_account.account_title AS federal_account_name,
            vw_appropriation_account_balances_download.budget_authority_unobligated_balance_brought_forward_fyb AS budget_authority_unobligated_balance_brought_forward,
            vw_appropriation_account_balances_download.adjustments_to_unobligated_balance_brought_forward_cpe AS adjustments_to_unobligated_balance_brought_forward_cpe,
            vw_appropriation_account_balances_download.budget_authority_appropriated_amount_cpe AS budget_authority_appropriated_amount,
            vw_appropriation_account_balances_download.borrowing_authority_amount_total_cpe AS borrowing_authority_amount,
            vw_appropriation_account_balances_download.contract_authority_amount_total_cpe AS contract_authority_amount,
            vw_appropriation_account_balances_download.spending_authority_from_offsetting_collections_amount_cpe AS spending_authority_from_offsetting_collections_amount,
            vw_appropriation_account_balances_download.other_budgetary_resources_amount_cpe AS total_other_budgetary_resources_amount,
            vw_appropriation_account_balances_download.total_budgetary_resources_amount_cpe AS total_budgetary_resources,
            vw_appropriation_account_balances_download.obligations_incurred_total_by_tas_cpe AS obligations_incurred,
            vw_appropriation_account_balances_download.deobligations_recoveries_refunds_by_tas_cpe AS deobligations_or_recoveries_or_refunds_from_prior_year,
            vw_appropriation_account_balances_download.unobligated_balance_cpe AS unobligated_balance,
            vw_appropriation_account_balances_download.gross_outlay_amount_by_tas_cpe AS gross_outlay_amount,
            vw_appropriation_account_balances_download.status_of_budgetary_resources_total_cpe AS status_of_budgetary_resources_total,
            (MAX(submission_attributes.published_date)) :: date AS last_modified_date
        FROM
            usas.public.vw_appropriation_account_balances_download
            INNER JOIN usas.public.submission_attributes ON (
                vw_appropriation_account_balances_download.submission_id = submission_attributes.submission_id
            )
            INNER JOIN usas.public.treasury_appropriation_account ON (
                vw_appropriation_account_balances_download.treasury_account_identifier = treasury_appropriation_account.treasury_account_identifier
            )
            INNER JOIN usas.public.toptier_agency ON (
                treasury_appropriation_account.funding_toptier_agency_id = toptier_agency.toptier_agency_id
            )
            LEFT OUTER JOIN usas.public.federal_account ON (
                treasury_appropriation_account.federal_account_id = federal_account.id
            )
        WHERE
            (
                vw_appropriation_account_balances_download.submission_id IN (
                    SELECT
                        submission_id
                    FROM
                        usas.public.submission_attributes
                    WHERE
                        (
                            toptier_code,
                            reporting_fiscal_year,
                            reporting_fiscal_period
                        ) IN (
                            SELECT
                                DISTINCT ON (toptier_code) toptier_code,
                                reporting_fiscal_year,
                                reporting_fiscal_period
                            FROM
                                usas.public.submission_attributes
                            WHERE
                                reporting_fiscal_year = 2023
                                AND (
                                    (
                                        reporting_fiscal_quarter <= 4
                                        AND quarter_format_flag IS TRUE
                                    )
                                    OR (
                                        reporting_fiscal_period <= 12
                                        AND quarter_format_flag IS FALSE
                                    )
                                )
                            ORDER BY
                                toptier_code,
                                reporting_fiscal_period DESC
                        )
                        AND (
                            (
                                reporting_fiscal_quarter = 4
                                AND quarter_format_flag IS TRUE
                            )
                            OR (
                                reporting_fiscal_period = 12
                                AND quarter_format_flag IS FALSE
                            )
                        )
                )
                AND treasury_appropriation_account.funding_toptier_agency_id = 63
            )
        GROUP BY
            vw_appropriation_account_balances_download.data_source,
            vw_appropriation_account_balances_download.appropriation_account_balances_id,
            vw_appropriation_account_balances_download.budget_authority_unobligated_balance_brought_forward_fyb,
            vw_appropriation_account_balances_download.adjustments_to_unobligated_balance_brought_forward_cpe,
            vw_appropriation_account_balances_download.budget_authority_appropriated_amount_cpe,
            vw_appropriation_account_balances_download.borrowing_authority_amount_total_cpe,
            vw_appropriation_account_balances_download.contract_authority_amount_total_cpe,
            vw_appropriation_account_balances_download.spending_authority_from_offsetting_collections_amount_cpe,
            vw_appropriation_account_balances_download.other_budgetary_resources_amount_cpe,
            vw_appropriation_account_balances_download.total_budgetary_resources_amount_cpe,
            vw_appropriation_account_balances_download.gross_outlay_amount_by_tas_cpe,
            vw_appropriation_account_balances_download.deobligations_recoveries_refunds_by_tas_cpe,
            vw_appropriation_account_balances_download.unobligated_balance_cpe,
            vw_appropriation_account_balances_download.status_of_budgetary_resources_total_cpe,
            vw_appropriation_account_balances_download.obligations_incurred_total_by_tas_cpe,
            vw_appropriation_account_balances_download.drv_appropriation_availability_period_start_date,
            vw_appropriation_account_balances_download.drv_appropriation_availability_period_end_date,
            vw_appropriation_account_balances_download.drv_appropriation_account_expired_status,
            vw_appropriation_account_balances_download.drv_obligations_unpaid_amount,
            vw_appropriation_account_balances_download.drv_other_obligated_amount,
            vw_appropriation_account_balances_download.reporting_period_start,
            vw_appropriation_account_balances_download.reporting_period_end,
            vw_appropriation_account_balances_download.last_modified_date,
            vw_appropriation_account_balances_download.certified_date,
            vw_appropriation_account_balances_download.create_date,
            vw_appropriation_account_balances_download.update_date,
            vw_appropriation_account_balances_download.final_of_fy,
            vw_appropriation_account_balances_download.submission_id,
            vw_appropriation_account_balances_download.treasury_account_identifier,
            vw_appropriation_account_balances_download.agency_identifier_name,
            vw_appropriation_account_balances_download.allocation_transfer_agency_identifier_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('Q') :: text,
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        ) :: varchar
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
                ELSE CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('P') :: text,
                                    (
                                        LPAD(
                                            (
                                                submission_attributes.reporting_fiscal_period
                                            ) :: varchar,
                                            2,
                                            '0'
                                        )
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
            END,
            toptier_agency.name,
            submission_attributes.reporting_agency_name,
            treasury_appropriation_account.allocation_transfer_agency_id,
            treasury_appropriation_account.agency_id,
            treasury_appropriation_account.beginning_period_of_availability,
            treasury_appropriation_account.ending_period_of_availability,
            treasury_appropriation_account.availability_type_code,
            treasury_appropriation_account.main_account_code,
            treasury_appropriation_account.sub_account_code,
            treasury_appropriation_account.tas_rendering_label,
            treasury_appropriation_account.account_title,
            treasury_appropriation_account.budget_function_title,
            treasury_appropriation_account.budget_subfunction_title,
            federal_account.federal_account_code,
            federal_account.account_title
        """
        conn.sql(file_a_ta_query).to_csv(f"{S3_DOWNLOAD_PATH}/file_a_{filename}", header=True)
        print(f"File A Treasury Account download generated in: {round(time.perf_counter() - start_time, 3)} seconds")

        ##########
        # File B #
        ##########
        print(f"\nStarting available memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
        print("Generating File B download")
        start_time = time.perf_counter()
        file_b_ta_query = """
        SELECT
            toptier_agency.name AS owning_agency_name,
            submission_attributes.reporting_agency_name AS reporting_agency_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('Q') :: text,
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        ) :: varchar
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
                ELSE CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('P') :: text,
                                    (
                                        LPAD(
                                            (
                                                submission_attributes.reporting_fiscal_period
                                            ) :: varchar,
                                            2,
                                            '0'
                                        )
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
            END AS submission_period,
            treasury_appropriation_account.allocation_transfer_agency_id AS allocation_transfer_agency_identifier_code,
            treasury_appropriation_account.agency_id AS agency_identifier_code,
            treasury_appropriation_account.beginning_period_of_availability AS beginning_period_of_availability,
            treasury_appropriation_account.ending_period_of_availability AS ending_period_of_availability,
            treasury_appropriation_account.availability_type_code AS availability_type_code,
            treasury_appropriation_account.main_account_code AS main_account_code,
            treasury_appropriation_account.sub_account_code AS sub_account_code,
            treasury_appropriation_account.tas_rendering_label AS treasury_account_symbol,
            treasury_appropriation_account.account_title AS treasury_account_name,
            vw_financial_accounts_by_program_activity_object_class_download.agency_identifier_name AS agency_identifier_name,
            vw_financial_accounts_by_program_activity_object_class_download.allocation_transfer_agency_identifier_name AS allocation_transfer_agency_identifier_name,
            treasury_appropriation_account.budget_function_title AS budget_function,
            treasury_appropriation_account.budget_subfunction_title AS budget_subfunction,
            federal_account.federal_account_code AS federal_account_symbol,
            federal_account.account_title AS federal_account_name,
            ref_program_activity.program_activity_code AS program_activity_code,
            ref_program_activity.program_activity_name AS program_activity_name,
            object_class.object_class AS object_class_code,
            object_class.object_class_name AS object_class_name,
            object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
            vw_financial_accounts_by_program_activity_object_class_download.disaster_emergency_fund_code AS disaster_emergency_fund_code,
            disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_incurred_by_program_object_class_cpe AS obligations_incurred,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_cpe AS obligations_undelivered_orders_unpaid_total,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_fyb AS obligations_undelivered_orders_unpaid_total_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_cpe AS USSGL480100_undelivered_orders_obligations_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_fyb AS USSGL480100_undelivered_orders_obligations_unpaid_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe AS USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_cpe AS obligations_delivered_orders_unpaid_total,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_cpe AS obligations_delivered_orders_unpaid_total_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_cpe AS USSGL490100_delivered_orders_obligations_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_fyb AS USSGL490100_delivered_orders_obligations_unpaid_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe AS USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_cpe AS gross_outlay_amount_FYB_to_period_end,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_fyb AS gross_outlay_amount_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_cpe AS gross_outlays_undelivered_orders_prepaid_total,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_cpe AS gross_outlays_undelivered_orders_prepaid_total_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe AS USSGL480200_undelivered_orders_obligations_prepaid_advanced,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb AS USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe AS USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_cpe AS gross_outlays_delivered_orders_paid_total,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_fyb AS gross_outlays_delivered_orders_paid_total_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490200_delivered_orders_obligations_paid_cpe AS USSGL490200_delivered_orders_obligations_paid,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_cpe AS USSGL490800_authority_outlayed_not_yet_disbursed,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_fyb AS USSGL490800_authority_outlayed_not_yet_disbursed_FYB,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe AS USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid,
            vw_financial_accounts_by_program_activity_object_class_download.deobligations_recoveries_refund_pri_program_object_class_cpe AS deobligations_or_recoveries_or_refunds_from_prior_year,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe AS USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe AS USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe AS USSGL483100_undelivered_orders_obligations_transferred_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe AS USSGL493100_delivered_orders_obligations_transferred_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe AS USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced,
            (MAX(submission_attributes.published_date)) :: date AS last_modified_date
        FROM
            usas.public.vw_financial_accounts_by_program_activity_object_class_download
            INNER JOIN usas.public.submission_attributes ON (
                vw_financial_accounts_by_program_activity_object_class_download.submission_id = submission_attributes.submission_id
            )
            INNER JOIN usas.public.treasury_appropriation_account ON (
                vw_financial_accounts_by_program_activity_object_class_download.treasury_account_id = treasury_appropriation_account.treasury_account_identifier
            )
            INNER JOIN usas.public.toptier_agency ON (
                treasury_appropriation_account.funding_toptier_agency_id = toptier_agency.toptier_agency_id
            )
            LEFT OUTER JOIN usas.public.ref_program_activity ON (
                vw_financial_accounts_by_program_activity_object_class_download.program_activity_id = ref_program_activity.id
            )
            LEFT OUTER JOIN usas.public.object_class ON (
                vw_financial_accounts_by_program_activity_object_class_download.object_class_id = object_class.id
            )
            LEFT OUTER JOIN usas.public.disaster_emergency_fund_code ON (
                vw_financial_accounts_by_program_activity_object_class_download.disaster_emergency_fund_code = disaster_emergency_fund_code.code
            )
            LEFT OUTER JOIN usas.public.federal_account ON (
                treasury_appropriation_account.federal_account_id = federal_account.id
            )
        WHERE
            (
                vw_financial_accounts_by_program_activity_object_class_download.submission_id IN (
                    SELECT
                        submission_id
                    FROM
                        usas.public.submission_attributes
                    WHERE
                        (
                            toptier_code,
                            reporting_fiscal_year,
                            reporting_fiscal_period
                        ) IN (
                            SELECT
                                DISTINCT ON (toptier_code) toptier_code,
                                reporting_fiscal_year,
                                reporting_fiscal_period
                            FROM
                                usas.public.submission_attributes
                            WHERE
                                reporting_fiscal_year = 2023
                                AND (
                                    (
                                        reporting_fiscal_quarter <= 4
                                        AND quarter_format_flag IS TRUE
                                    )
                                    OR (
                                        reporting_fiscal_period <= 12
                                        AND quarter_format_flag IS FALSE
                                    )
                                )
                            ORDER BY
                                toptier_code,
                                reporting_fiscal_period DESC
                        )
                        AND (
                            (
                                reporting_fiscal_quarter = 4
                                AND quarter_format_flag IS TRUE
                            )
                            OR (
                                reporting_fiscal_period = 12
                                AND quarter_format_flag IS FALSE
                            )
                        )
                )
                AND treasury_appropriation_account.funding_toptier_agency_id = 63
            )
        GROUP BY
            vw_financial_accounts_by_program_activity_object_class_download.data_source,
            vw_financial_accounts_by_program_activity_object_class_download.financial_accounts_by_program_activity_object_class_id,
            vw_financial_accounts_by_program_activity_object_class_download.program_activity_id,
            vw_financial_accounts_by_program_activity_object_class_download.object_class_id,
            vw_financial_accounts_by_program_activity_object_class_download.prior_year_adjustment,
            vw_financial_accounts_by_program_activity_object_class_download.program_activity_reporting_key,
            vw_financial_accounts_by_program_activity_object_class_download.disaster_emergency_fund_code,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480110_rein_undel_ord_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490110_rein_deliv_ord_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490200_delivered_orders_obligations_paid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_fyb,
            vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.obligations_incurred_by_program_object_class_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.deobligations_recoveries_refund_pri_program_object_class_cpe,
            vw_financial_accounts_by_program_activity_object_class_download.drv_obligations_incurred_by_program_object_class,
            vw_financial_accounts_by_program_activity_object_class_download.drv_obligations_undelivered_orders_unpaid,
            vw_financial_accounts_by_program_activity_object_class_download.reporting_period_start,
            vw_financial_accounts_by_program_activity_object_class_download.reporting_period_end,
            vw_financial_accounts_by_program_activity_object_class_download.last_modified_date,
            vw_financial_accounts_by_program_activity_object_class_download.certified_date,
            vw_financial_accounts_by_program_activity_object_class_download.create_date,
            vw_financial_accounts_by_program_activity_object_class_download.update_date,
            vw_financial_accounts_by_program_activity_object_class_download.submission_id,
            vw_financial_accounts_by_program_activity_object_class_download.treasury_account_id,
            vw_financial_accounts_by_program_activity_object_class_download.agency_identifier_name,
            vw_financial_accounts_by_program_activity_object_class_download.allocation_transfer_agency_identifier_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('Q') :: text,
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        ) :: varchar
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
                ELSE CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('P') :: text,
                                    (
                                        LPAD(
                                            (
                                                submission_attributes.reporting_fiscal_period
                                            ) :: varchar,
                                            2,
                                            '0'
                                        )
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
            END,
            toptier_agency.name,
            submission_attributes.reporting_agency_name,
            treasury_appropriation_account.allocation_transfer_agency_id,
            treasury_appropriation_account.agency_id,
            treasury_appropriation_account.beginning_period_of_availability,
            treasury_appropriation_account.ending_period_of_availability,
            treasury_appropriation_account.availability_type_code,
            treasury_appropriation_account.main_account_code,
            treasury_appropriation_account.sub_account_code,
            treasury_appropriation_account.tas_rendering_label,
            treasury_appropriation_account.account_title,
            treasury_appropriation_account.budget_function_title,
            treasury_appropriation_account.budget_subfunction_title,
            federal_account.federal_account_code,
            federal_account.account_title,
            ref_program_activity.program_activity_code,
            ref_program_activity.program_activity_name,
            object_class.object_class,
            object_class.object_class_name,
            object_class.direct_reimbursable,
            disaster_emergency_fund_code.title
        """
        conn.sql(file_b_ta_query).to_csv(f"{S3_DOWNLOAD_PATH}/file_b_{filename}", header=True)
        print(f"File B Treasury Account download generated in: {round(time.perf_counter() - start_time, 3)} seconds")

        ##########
        # File C #
        ##########
        print(f"\nStarting available memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
        print("Generating File C download")
        start_time = time.perf_counter()
        file_c_ta_query = """
        SELECT
            toptier_agency.name AS owning_agency_name,
            submission_attributes.reporting_agency_name AS reporting_agency_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('Q') :: text,
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        ) :: varchar
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
                ELSE CONCAT(
                    ('FY') :: text,
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year) :: varchar
                            ) :: text,
                            (
                                CONCAT(
                                    ('P') :: text,
                                    (
                                        LPAD(
                                            (
                                                submission_attributes.reporting_fiscal_period
                                            ) :: varchar,
                                            2,
                                            '0'
                                        )
                                    ) :: text
                                )
                            ) :: text
                        )
                    ) :: text
                )
            END AS submission_period,
            treasury_appropriation_account.allocation_transfer_agency_id AS allocation_transfer_agency_identifier_code,
            treasury_appropriation_account.agency_id AS agency_identifier_code,
            treasury_appropriation_account.beginning_period_of_availability AS beginning_period_of_availability,
            treasury_appropriation_account.ending_period_of_availability AS ending_period_of_availability,
            treasury_appropriation_account.availability_type_code AS availability_type_code,
            treasury_appropriation_account.main_account_code AS main_account_code,
            treasury_appropriation_account.sub_account_code AS sub_account_code,
            treasury_appropriation_account.tas_rendering_label AS treasury_account_symbol,
            treasury_appropriation_account.account_title AS treasury_account_name,
            vw_financial_accounts_by_awards_download.agency_identifier_name AS agency_identifier_name,
            vw_financial_accounts_by_awards_download.allocation_transfer_agency_identifier_name AS allocation_transfer_agency_identifier_name,
            treasury_appropriation_account.budget_function_title AS budget_function,
            treasury_appropriation_account.budget_subfunction_title AS budget_subfunction,
            federal_account.federal_account_code AS federal_account_symbol,
            federal_account.account_title AS federal_account_name,
            ref_program_activity.program_activity_code AS program_activity_code,
            ref_program_activity.program_activity_name AS program_activity_name,
            object_class.object_class AS object_class_code,
            object_class.object_class_name AS object_class_name,
            object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
            vw_financial_accounts_by_awards_download.disaster_emergency_fund_code AS disaster_emergency_fund_code,
            disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
            vw_financial_accounts_by_awards_download.transaction_obligated_amount AS transaction_obligated_amount,
            vw_financial_accounts_by_awards_download.gross_outlay_amount_by_award_cpe AS gross_outlay_amount_FYB_to_period_end,
            vw_financial_accounts_by_awards_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
            vw_financial_accounts_by_awards_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
            award_search.generated_unique_award_id AS award_unique_key,
            vw_financial_accounts_by_awards_download.piid AS award_id_piid,
            vw_financial_accounts_by_awards_download.parent_award_id AS parent_award_id_piid,
            vw_financial_accounts_by_awards_download.fain AS award_id_fain,
            vw_financial_accounts_by_awards_download.uri AS award_id_uri,
            award_search.date_signed AS award_base_action_date,
            EXTRACT(
                YEAR
                from
                    (award_search.date_signed) + INTERVAL '3 months'
            ) AS award_base_action_date_fiscal_year,
            award_search.certified_date AS award_latest_action_date,
            EXTRACT(
                YEAR
                from
                    (award_search.certified_date) + INTERVAL '3 months'
            ) AS award_latest_action_date_fiscal_year,
            award_search.period_of_performance_start_date AS period_of_performance_start_date,
            award_search.period_of_performance_current_end_date AS period_of_performance_current_end_date,
            transaction_search.ordering_period_end_date AS ordering_period_end_date,
            COALESCE(
                transaction_search.contract_award_type,
                transaction_search.type
            ) AS award_type_code,
            COALESCE(
                transaction_search.contract_award_type_desc,
                transaction_search.type_description
            ) AS award_type,
            transaction_search.idv_type AS idv_type_code,
            transaction_search.idv_type_description AS idv_type,
            award_search.description AS prime_award_base_transaction_description,
            transaction_search.awarding_agency_code AS awarding_agency_code,
            transaction_search.awarding_toptier_agency_name_raw AS awarding_agency_name,
            transaction_search.awarding_sub_tier_agency_c AS awarding_subagency_code,
            transaction_search.awarding_subtier_agency_name_raw AS awarding_subagency_name,
            transaction_search.awarding_office_code AS awarding_office_code,
            transaction_search.awarding_office_name AS awarding_office_name,
            transaction_search.funding_agency_code AS funding_agency_code,
            transaction_search.funding_toptier_agency_name_raw AS funding_agency_name,
            transaction_search.funding_sub_tier_agency_co AS funding_sub_agency_code,
            transaction_search.funding_subtier_agency_name_raw AS funding_sub_agency_name,
            transaction_search.funding_office_code AS funding_office_code,
            transaction_search.funding_office_name AS funding_office_name,
            transaction_search.recipient_uei AS recipient_uei,
            transaction_search.recipient_unique_id AS recipient_duns,
            transaction_search.recipient_name AS recipient_name,
            transaction_search.recipient_name_raw AS recipient_name_raw,
            transaction_search.parent_uei AS recipient_parent_uei,
            transaction_search.parent_uei AS recipient_parent_duns,
            transaction_search.parent_recipient_name AS recipient_parent_name,
            transaction_search.parent_recipient_name_raw AS recipient_parent_name_raw,
            transaction_search.recipient_location_country_code AS recipient_country,
            transaction_search.recipient_location_state_code AS recipient_state,
            transaction_search.recipient_location_county_name AS recipient_county,
            transaction_search.recipient_location_city_name AS recipient_city,
            CASE
                WHEN (
                    transaction_search.recipient_location_state_code IS NOT NULL
                    AND transaction_search.recipient_location_congressional_code IS NOT NULL
                    AND NOT (
                        transaction_search.recipient_location_state_code = ''
                        AND transaction_search.recipient_location_state_code IS NOT NULL
                    )
                ) THEN CONCAT(
                    transaction_search.recipient_location_state_code,
                    '-',
                    transaction_search.recipient_location_congressional_code
                )
                ELSE transaction_search.recipient_location_congressional_code
            END AS prime_award_summary_recipient_cd_original,
            CASE
                WHEN (
                    transaction_search.recipient_location_state_code IS NOT NULL
                    AND transaction_search.recipient_location_congressional_code_current IS NOT NULL
                    AND NOT (
                        transaction_search.recipient_location_state_code = ''
                        AND transaction_search.recipient_location_state_code IS NOT NULL
                    )
                ) THEN CONCAT(
                    transaction_search.recipient_location_state_code,
                    '-',
                    transaction_search.recipient_location_congressional_code_current
                )
                ELSE transaction_search.recipient_location_congressional_code_current
            END AS prime_award_summary_recipient_cd_current,
            COALESCE(
                transaction_search.legal_entity_zip4,
                CONCAT(
                    (transaction_search.recipient_location_zip5) :: text,
                    (transaction_search.legal_entity_zip_last4) :: text
                )
            ) AS recipient_zip_code,
            transaction_search.pop_country_name AS primary_place_of_performance_country,
            transaction_search.pop_state_name AS primary_place_of_performance_state,
            transaction_search.pop_county_name AS primary_place_of_performance_county,
            CASE
                WHEN (
                    transaction_search.pop_state_code IS NOT NULL
                    AND transaction_search.pop_congressional_code IS NOT NULL
                    AND NOT (
                        transaction_search.pop_state_code = ''
                        AND transaction_search.pop_state_code IS NOT NULL
                    )
                ) THEN CONCAT(
                    transaction_search.pop_state_code,
                    '-',
                    transaction_search.pop_congressional_code
                )
                ELSE transaction_search.pop_congressional_code
            END AS prime_award_summary_place_of_performance_cd_original,
            CASE
                WHEN (
                    transaction_search.pop_state_code IS NOT NULL
                    AND transaction_search.pop_congressional_code_current IS NOT NULL
                    AND NOT (
                        transaction_search.pop_state_code = ''
                        AND transaction_search.pop_state_code IS NOT NULL
                    )
                ) THEN CONCAT(
                    transaction_search.pop_state_code,
                    '-',
                    transaction_search.pop_congressional_code_current
                )
                ELSE transaction_search.pop_congressional_code_current
            END AS prime_award_summary_place_of_performance_cd_current,
            transaction_search.place_of_performance_zip4a AS primary_place_of_performance_zip_code,
            transaction_search.cfda_number AS cfda_number,
            transaction_search.cfda_title AS cfda_title,
            transaction_search.product_or_service_code AS product_or_service_code,
            transaction_search.product_or_service_description AS product_or_service_code_description,
            transaction_search.naics_code AS naics_code,
            transaction_search.naics_description AS naics_description,
            transaction_search.national_interest_action AS national_interest_action_code,
            transaction_search.national_interest_desc AS national_interest_action,
            CASE
                WHEN award_search.generated_unique_award_id IS NOT NULL THEN CONCAT(
                    'localhost:3000/award/',
                    URL_ENCODE(award_search.generated_unique_award_id),
                    '/'
                )
                ELSE ''
            END AS usaspending_permalink,
            (submission_attributes.published_date) :: date AS last_modified_date
        FROM
            usas.public.vw_financial_accounts_by_awards_download
            INNER JOIN usas.public.submission_attributes ON (
                vw_financial_accounts_by_awards_download.submission_id = submission_attributes.submission_id
            )
            INNER JOIN usas.rpt.award_search ON (
                vw_financial_accounts_by_awards_download.award_id = award_search.award_id
            )
            INNER JOIN usas.rpt.transaction_search ON (
                award_search.latest_transaction_search_id = transaction_search.transaction_id
            )
            INNER JOIN usas.public.treasury_appropriation_account ON (
                vw_financial_accounts_by_awards_download.treasury_account_id = treasury_appropriation_account.treasury_account_identifier
            )
            INNER JOIN usas.public.toptier_agency ON (
                treasury_appropriation_account.funding_toptier_agency_id = toptier_agency.toptier_agency_id
            )
            LEFT OUTER JOIN usas.public.federal_account ON (
                treasury_appropriation_account.federal_account_id = federal_account.id
            )
            LEFT OUTER JOIN usas.public.ref_program_activity ON (
                vw_financial_accounts_by_awards_download.program_activity_id = ref_program_activity.id
            )
            LEFT OUTER JOIN usas.public.object_class ON (
                vw_financial_accounts_by_awards_download.object_class_id = object_class.id
            )
            LEFT OUTER JOIN usas.public.disaster_emergency_fund_code ON (
                vw_financial_accounts_by_awards_download.disaster_emergency_fund_code = disaster_emergency_fund_code.code
            )
        WHERE
            (
                (
                    (
                        submission_attributes.reporting_fiscal_period <= 12
                        AND NOT submission_attributes.quarter_format_flag
                    )
                    OR (
                        submission_attributes.reporting_fiscal_quarter <= 4
                        AND submission_attributes.quarter_format_flag
                    )
                )
                AND submission_attributes.reporting_fiscal_year = 2023
                AND (
                    vw_financial_accounts_by_awards_download.gross_outlay_amount_by_award_cpe > 0
                    OR vw_financial_accounts_by_awards_download.gross_outlay_amount_by_award_cpe < 0
                    OR vw_financial_accounts_by_awards_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe > 0
                    OR vw_financial_accounts_by_awards_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe < 0
                    OR vw_financial_accounts_by_awards_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe > 0
                    OR vw_financial_accounts_by_awards_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe < 0
                    OR vw_financial_accounts_by_awards_download.transaction_obligated_amount > 0
                    OR vw_financial_accounts_by_awards_download.transaction_obligated_amount < 0
                )
                AND transaction_search.is_fpds
                AND treasury_appropriation_account.funding_toptier_agency_id = 63
            )
        """
        conn.sql(file_c_ta_query).to_csv(f"{S3_DOWNLOAD_PATH}/file_c_{filename}", header=True)
        print(f"File C Treasury Account download generated in: {round(time.perf_counter() - start_time, 3)} seconds")
