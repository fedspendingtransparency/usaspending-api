import os
import time

import duckdb
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
                print(f"Loading extension: {extension}")
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
            print(f"Successfully loaded: {extension}\n")

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

        print("Generating File B download")
        start_time = time.perf_counter()
        conn.execute(f"ATTACH '{os.getenv('DATABASE_URL')}' AS usas (TYPE postgres, READ_ONLY);")

        file_b_download_sql = """
        SELECT
            toptier_agency.name AS owning_agency_name,
            STRING_AGG(
                DISTINCT submission_attributes.reporting_agency_name,
                '; '
            ) AS reporting_agency_name,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY'),
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year)
                            ),
                            (
                                CONCAT(
                                    ('Q'),
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                ELSE CONCAT(
                    ('FY'),
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year)
                            ),
                            (
                                CONCAT(
                                    ('P'),
                                    (
                                        LPAD(
                                            CAST(submission_attributes.reporting_fiscal_period AS VARCHAR),
                                            2,
                                            CAST('0' AS VARCHAR)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            END AS submission_period,
            vw_financial_accounts_by_program_activity_object_class_download.agency_identifier_name AS agency_identifier_name,
            STRING_AGG(
                DISTINCT treasury_appropriation_account.budget_function_title,
                '; '
            ) AS budget_function,
            STRING_AGG(
                DISTINCT treasury_appropriation_account.budget_subfunction_title,
                '; '
            ) AS budget_subfunction,
            federal_account.federal_account_code AS federal_account_symbol,
            federal_account.account_title AS federal_account_name,
            ref_program_activity.program_activity_code AS program_activity_code,
            ref_program_activity.program_activity_name AS program_activity_name,
            object_class.object_class AS object_class_code,
            object_class.object_class_name AS object_class_name,
            object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
            vw_financial_accounts_by_program_activity_object_class_download.disaster_emergency_fund_code AS disaster_emergency_fund_code,
            disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.obligations_incurred_by_program_object_class_cpe
            ) AS obligations_incurred,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_cpe
            ) AS obligations_undelivered_orders_unpaid_total,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.obligations_undelivered_orders_unpaid_total_fyb
            ) AS obligations_undelivered_orders_unpaid_total_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_cpe
            ) AS USSGL480100_undelivered_orders_obligations_unpaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl480100_undelivered_orders_obligations_unpaid_fyb
            ) AS USSGL480100_undelivered_orders_obligations_unpaid_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe
            ) AS USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_cpe
            ) AS obligations_delivered_orders_unpaid_total,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.obligations_delivered_orders_unpaid_total_cpe
            ) AS obligations_delivered_orders_unpaid_total_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_cpe
            ) AS USSGL490100_delivered_orders_obligations_unpaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl490100_delivered_orders_obligations_unpaid_fyb
            ) AS USSGL490100_delivered_orders_obligations_unpaid_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe
            ) AS USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid,
            SUM(
                CASE
                    WHEN (
                        (
                            (
                                submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_quarter = 4
                            )
                            OR (
                                NOT submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_period = 12
                            )
                        )
                        AND submission_attributes.reporting_fiscal_year = 2021
                    ) THEN vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_cpe
                    ELSE (NULL) :: NUMERIC(23, 2)
                END
            ) AS gross_outlay_amount_FYB_to_period_end,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.gross_outlay_amount_by_program_object_class_fyb
            ) AS gross_outlay_amount_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_cpe
            ) AS gross_outlays_undelivered_orders_prepaid_total,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_undelivered_orders_prepaid_total_cpe
            ) AS gross_outlays_undelivered_orders_prepaid_total_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe
            ) AS USSGL480200_undelivered_orders_obligations_prepaid_advanced,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb
            ) AS USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe
            ) AS USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_cpe
            ) AS gross_outlays_delivered_orders_paid_total,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.gross_outlays_delivered_orders_paid_total_fyb
            ) AS gross_outlays_delivered_orders_paid_total_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl490200_delivered_orders_obligations_paid_cpe
            ) AS USSGL490200_delivered_orders_obligations_paid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_cpe
            ) AS USSGL490800_authority_outlayed_not_yet_disbursed,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl490800_authority_outlayed_not_yet_disbursed_fyb
            ) AS USSGL490800_authority_outlayed_not_yet_disbursed_FYB,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe
            ) AS USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.deobligations_recoveries_refund_pri_program_object_class_cpe
            ) AS deobligations_or_recoveries_or_refunds_from_prior_year,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe
            ) AS USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe
            ) AS USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig,
            SUM(
                CASE
                    WHEN (
                        (
                            (
                                submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_quarter = 4
                            )
                            OR (
                                NOT submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_period = 12
                            )
                        )
                        AND submission_attributes.reporting_fiscal_year = 2021
                    ) THEN vw_financial_accounts_by_program_activity_object_class_download.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                    ELSE (NULL) :: NUMERIC(23, 2)
                END
            ) AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
            SUM(
                CASE
                    WHEN (
                        (
                            (
                                submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_quarter = 4
                            )
                            OR (
                                NOT submission_attributes.quarter_format_flag
                                AND submission_attributes.reporting_fiscal_period = 12
                            )
                        )
                        AND submission_attributes.reporting_fiscal_year = 2021
                    ) THEN vw_financial_accounts_by_program_activity_object_class_download.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
                    ELSE (NULL) :: NUMERIC(23, 2)
                END
            ) AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe
            ) AS USSGL483100_undelivered_orders_obligations_transferred_unpaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe
            ) AS USSGL493100_delivered_orders_obligations_transferred_unpaid,
            SUM(
                vw_financial_accounts_by_program_activity_object_class_download.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe
            ) AS USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced,
            (MAX(submission_attributes.published_date)) :: DATE AS last_modified_date
        FROM
            usas.public.vw_financial_accounts_by_program_activity_object_class_download
            INNER JOIN usas.public.submission_attributes ON (
                vw_financial_accounts_by_program_activity_object_class_download.submission_id = submission_attributes.submission_id
            )
            LEFT OUTER JOIN usas.public.treasury_appropriation_account ON (
                vw_financial_accounts_by_program_activity_object_class_download.treasury_account_id = treasury_appropriation_account.treasury_account_identifier
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
            LEFT OUTER JOIN usas.public.toptier_agency ON (
                federal_account.parent_toptier_agency_id = toptier_agency.toptier_agency_id
            )
        WHERE
            vw_financial_accounts_by_program_activity_object_class_download.submission_id IN (
                43601,
                43898,
                44151,
                43780,
                43963,
                44188,
                43775,
                44134,
                44148,
                43472,
                43948,
                43273,
                43290,
                43308,
                43322,
                43373,
                43390,
                43697,
                43489,
                43499,
                43503,
                43520,
                43604,
                43606,
                43716,
                43742,
                43796,
                43824,
                43901,
                43910,
                44032,
                44035,
                44067,
                44073,
                44096,
                44102,
                44150,
                44131,
                43590,
                43159,
                43556,
                43583,
                43593,
                43270,
                43527,
                43599,
                43598,
                43732,
                43307,
                43318,
                43474,
                43455,
                43302,
                43243,
                43478,
                43303,
                43385,
                43513,
                43269,
                43310,
                43563,
                43381,
                43778,
                43733,
                43658,
                43309,
                43721,
                43306,
                44010,
                43629,
                44091,
                43695,
                43326,
                43395,
                43398,
                43471,
                44008,
                43976,
                43205,
                43234,
                43325,
                43377,
                43479,
                43741,
                43744,
                43745,
                43748,
                43977,
                44027,
                44056,
                44118,
                43866,
                43940,
                43603,
                44038,
                43532,
                43609,
                44111,
                43524,
                43562,
                44076,
                43592,
                43607,
                43594,
                43988
            )
        GROUP BY
            toptier_agency.name,
            vw_financial_accounts_by_program_activity_object_class_download.agency_identifier_name,
            federal_account.federal_account_code,
            federal_account.account_title,
            ref_program_activity.program_activity_code,
            ref_program_activity.program_activity_name,
            object_class.object_class,
            object_class.object_class_name,
            object_class.direct_reimbursable,
            vw_financial_accounts_by_program_activity_object_class_download.disaster_emergency_fund_code,
            disaster_emergency_fund_code.title,
            CASE
                WHEN submission_attributes.quarter_format_flag THEN CONCAT(
                    ('FY'),
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year)
                            ),
                            (
                                CONCAT(
                                    ('Q'),
                                    (
                                        (
                                            submission_attributes.reporting_fiscal_quarter
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                ELSE CONCAT(
                    ('FY'),
                    (
                        CONCAT(
                            (
                                (submission_attributes.reporting_fiscal_year)
                            ),
                            (
                                CONCAT('P', LPAD(
                                    CAST(submission_attributes.reporting_fiscal_period AS VARCHAR),
                                    2,
                                    CAST('0' AS VARCHAR)
                                ))
                            )
                        )
                    )
                )
            END
        """

        conn.sql(file_b_download_sql).to_csv(f"{S3_DOWNLOAD_PATH}/file_b_download_duckdb.csv", header=True)
        print(f"File B download generated in: {round(time.perf_counter() - start_time, 3)} seconds")
        print(f"File B download saved in {S3_DOWNLOAD_PATH}/file_b_download_duckdb.csv")
