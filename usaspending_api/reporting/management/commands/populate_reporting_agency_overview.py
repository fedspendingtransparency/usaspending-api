import logging

from enum import Enum

from django.core.management import BaseCommand
from django.db import transaction, connection

from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("script")


class TempTableName(Enum):
    VALID_FILE_C = "temp_valid_file_c_awards"
    VALID_FILE_D = "temp_valid_file_d_awards"
    QUARTERLY_LOOKUP = "temp_quarterly_submission_lookup"
    REPORTING_OVERVIEW = "temp_reporting_agency_overview"
    AWARD_COUNTS = "temp_reporting_agency_award_counts"


OVERVIEW_TABLE_NAME = "reporting_agency_overview"


"""
STEPS:
- Create temporary tables:
    - Valid File D
    - Valid File C
    - Unlinked File C
    - Unlinked File D
    - Linked File C and File D Awards
- Create indexes on tables to increase performance
    - Should test with and without the indexes
- Load Valid File C
    - include quarter format flag to determine period (when possible) for Valid File D
- Load Valid File D
    - only include Period, but use CASE statement to determine final value
- In parallel
    - Load Unlinked File C
    - Load Unlinked File D
    - Linked Awards
- Create Reporting Agency Overview table
    - Maybe use CTE to create in memory?
    - Join Reporting Agency Overview to counts from Unlinked File C, Unlinked File D, and Linked Awards
"""

CREATE_AND_PREP_TEMP_TABLES = f"""
    DROP TABLE IF EXISTS {TempTableName.VALID_FILE_C.value};
    DROP TABLE IF EXISTS {TempTableName.VALID_FILE_D.value};
    DROP TABLE IF EXISTS {TempTableName.QUARTERLY_LOOKUP.value};
    DROP TABLE IF EXISTS {TempTableName.REPORTING_OVERVIEW.value};
    DROP TABLE IF EXISTS {TempTableName.AWARD_COUNTS.value};

    CREATE TEMPORARY TABLE {TempTableName.VALID_FILE_C.value} (
        toptier_code TEXT,
        award_id INTEGER,
        distinct_award_key TEXT,
        is_fpds BOOLEAN,
        fiscal_year INTEGER,
        fiscal_quarter INTEGER,
        fiscal_period INTEGER,
        quarter_format_flag BOOLEAN
    );

    CREATE TEMPORARY TABLE {TempTableName.QUARTERLY_LOOKUP.value} (
        toptier_code TEXT,
        fiscal_year INTEGER,
        fiscal_quarter INTEGER,
        quarter_format_flag BOOLEAN
    );

    CREATE TEMPORARY TABLE {TempTableName.VALID_FILE_D.value} (
        toptier_code TEXT,
        award_id INTEGER,
        is_fpds BOOLEAN,
        fiscal_year INTEGER,
        fiscal_period INTEGER
    );

    CREATE TEMPORARY TABLE {TempTableName.REPORTING_OVERVIEW.value} (
        fiscal_period INTEGER,
        fiscal_year INTEGER,
        toptier_code TEXT,
        total_dollars_obligated_gtas NUMERIC(23, 2),
        total_budgetary_resources NUMERIC(23, 2),
        total_diff_approp_ocpa_obligated_amounts NUMERIC(23, 2)
    );

    CREATE TEMPORARY TABLE {TempTableName.AWARD_COUNTS.value} (
        toptier_code TEXT,
        fiscal_year INTEGER,
        fiscal_period INTEGER,
        unlinked_procurement_c_awards INTEGER,
        unlinked_assistance_c_awards INTEGER,
        unlinked_d1_awards INTEGER,
        unlinked_d2_awards INTEGER,
        linked_procurement_awards INTEGER,
        linked_assistance_awards INTEGER
    );

    ----- Any Indexes to increase performance go below here -----
"""

TEMP_TABLE_CONTENTS = {
    TempTableName.VALID_FILE_C: """
        SELECT DISTINCT
            ta.toptier_code,
            faba.award_id,
            faba.distinct_award_key,
            CASE WHEN faba.piid IS NULL THEN true ELSE false END AS is_fpds,
            sa.reporting_fiscal_year AS fiscal_year,
            sa.reporting_fiscal_quarter as fiscal_quarter,
            sa.reporting_fiscal_period AS fiscal_period,
            sa.quarter_format_flag
        FROM
            financial_accounts_by_awards AS faba
        INNER JOIN
            submission_attributes AS sa USING (submission_id)
        INNER JOIN
            dabs_submission_window_schedule AS dsws ON (
                sa.submission_window_id = dsws.id
                AND dsws.submission_reveal_date <= now()
            )
        INNER JOIN
            treasury_appropriation_account AS taa ON (taa.treasury_account_identifier = faba.treasury_account_id)
        INNER JOIN
            toptier_agency AS ta ON (taa.funding_toptier_agency_id = ta.toptier_agency_id)
        WHERE
            faba.transaction_obligated_amount IS NOT NULL
            AND sa.reporting_fiscal_year >= 2017
    """,
    TempTableName.QUARTERLY_LOOKUP: f"""
        SELECT DISTINCT
            toptier_code,
            fiscal_year,
            fiscal_quarter,
            quarter_format_flag
        FROM
            {TempTableName.VALID_FILE_C.value}
        WHERE
            quarter_format_flag = true
    """,
    TempTableName.VALID_FILE_D: f"""
        SELECT DISTINCT
            fa.toptier_code,
            awards.id AS award_id,
            awards.is_fpds,
            transactions.fiscal_year,
            CASE
                WHEN ql.quarter_format_flag = true THEN transactions.fiscal_quarter * 3
                WHEN transactions.fiscal_period = 1 THEN 2
                ELSE transactions.fiscal_period
            END AS fiscal_period
        FROM
            awards
        INNER JOIN
            (
                SELECT
                    tn.award_id,
                    fiscal_year,
                    date_part('quarter', tn.action_date + INTERVAL '3' MONTH) AS fiscal_quarter,
                    date_part('month', tn.action_date + interval '3' MONTH) AS fiscal_period
                FROM
                    transaction_normalized AS tn
                WHERE
                    tn.action_date >= '2016-10-01'
                    AND tn.funding_agency_id IS NOT NULL
            ) AS transactions ON (transactions.award_id = awards.id)
        INNER JOIN LATERAL (
            SELECT ta.toptier_code
            FROM agency AS ag
            INNER JOIN toptier_agency AS ta ON (ag.toptier_agency_id = ta.toptier_agency_id)
            WHERE ag.id = awards.funding_agency_id
        ) AS fa ON true
        LEFT OUTER JOIN
            {TempTableName.QUARTERLY_LOOKUP.value} AS ql ON (
                fa.toptier_code = ql.toptier_code
                AND transactions.fiscal_year = ql.fiscal_year
                AND transactions.fiscal_quarter = ql.fiscal_quarter
            )
        WHERE
            (
                (awards.type IN ('07', '08') AND awards.total_subsidy_cost > 0)
                OR awards.type NOT IN ('07', '08')
            ) AND awards.certified_date >= '2016-10-01'
    """,
    TempTableName.REPORTING_OVERVIEW: f"""
        WITH sum_reporting_agency_tas AS (
            SELECT
                fiscal_period,
                fiscal_year,
                toptier_code,
                SUM(diff_approp_ocpa_obligated_amounts) AS total_diff_approp_ocpa_obligated_amounts
            FROM
                reporting_agency_tas
            GROUP BY
                fiscal_period,
                fiscal_year,
                toptier_code
        ),
        sum_budgetary_resources AS (
            SELECT
                reporting_fiscal_period,
                reporting_fiscal_year,
                ta.toptier_code,
                SUM(total_budgetary_resources_amount_cpe) AS total_budgetary_resources
            FROM
                appropriation_account_balances AS aab
            INNER JOIN
                submission_attributes AS sa ON (sa.submission_id = aab.submission_id)
            INNER JOIN
                treasury_appropriation_account AS taa
                    ON (aab.treasury_account_identifier = taa.treasury_account_identifier)
            INNER JOIN
                toptier_agency AS ta ON (taa.funding_toptier_agency_id = ta.toptier_agency_id)
            GROUP BY
                reporting_fiscal_year,
                reporting_fiscal_period,
                ta.toptier_code
        ),
        sum_gtas_obligations AS (
            SELECT
                gtas.fiscal_year,
                gtas.fiscal_period,
                toptier_code,
                SUM(obligations_incurred_total_cpe) AS total_dollars_obligated_gtas
            FROM
                gtas_sf133_balances AS gtas
            INNER JOIN
                treasury_appropriation_account AS taa
                    ON (gtas.treasury_account_identifier = taa.treasury_account_identifier)
            INNER JOIN
                toptier_agency AS ta ON (taa.funding_toptier_agency_id = ta.toptier_agency_id)
            GROUP BY
                fiscal_year,
                fiscal_period,
                toptier_code
        )
        SELECT
            srat.fiscal_period,
            srat.fiscal_year,
            srat.toptier_code,
            COALESCE(total_dollars_obligated_gtas, 0) AS total_dollars_obligated_gtas,
            total_budgetary_resources,
            total_diff_approp_ocpa_obligated_amounts
        FROM
            sum_reporting_agency_tas AS srat
        INNER JOIN
            sum_budgetary_resources AS sbr ON (
                sbr.reporting_fiscal_year = srat.fiscal_year
                AND sbr.reporting_fiscal_period = srat.fiscal_period
                AND sbr.toptier_code = srat.toptier_code
            )
        LEFT OUTER JOIN
            sum_gtas_obligations AS sgo ON (
                sgo.fiscal_year = srat.fiscal_year
                AND sgo.fiscal_period = srat.fiscal_period
                AND sgo.toptier_code = srat.toptier_code
            )
    """,
    TempTableName.AWARD_COUNTS: f"""
        WITH unlinked_file_c_awards AS (
            SELECT
                toptier_code,
                fiscal_year,
                fiscal_period,
                COUNT(CASE WHEN is_fpds = True THEN 1 ELSE NULL END) AS procurement,
                COUNT(CASE WHEN is_fpds = False THEN 1 ELSE NULL END) AS assistance
            FROM
                {TempTableName.VALID_FILE_C.value}
            WHERE
                award_id IS NULL
            GROUP BY
                toptier_code,
                fiscal_year,
                fiscal_period
        ),
        unlinked_file_d_awards AS (
            SELECT
                toptier_code,
                fiscal_year,
                fiscal_period,
                COUNT(CASE WHEN is_fpds = True THEN 1 ELSE NULL END) AS procurement,
                COUNT(CASE WHEN is_fpds = False THEN 1 ELSE NULL END) AS assistance
            FROM
                {TempTableName.VALID_FILE_D.value}
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM financial_accounts_by_awards AS faba
                    WHERE faba.award_id = award_id
                )
            GROUP BY
                toptier_code,
                fiscal_year,
                fiscal_period
        ),
        linked_file_c_and_d_awards AS (
            SELECT
                toptier_code,
                fiscal_year,
                fiscal_period,
                COUNT(CASE WHEN is_fpds = True THEN 1 ELSE NULL END) AS procurement,
                COUNT(CASE WHEN is_fpds = False THEN 1 ELSE NULL END) AS assistance
            FROM (
                SELECT
                    toptier_code,
                    is_fpds,
                    award_id,
                    fiscal_year,
                    fiscal_period
                FROM
                    {TempTableName.VALID_FILE_C.value} AS vfc
                WHERE
                    vfc.award_id IS NOT NULL
                UNION
                SELECT
                    toptier_code,
                    is_fpds,
                    award_id,
                    fiscal_year,
                    fiscal_period
                FROM
                    {TempTableName.VALID_FILE_D.value} AS vfd
                WHERE
                    EXISTS (
                        SELECT 1
                        FROM financial_accounts_by_awards AS faba
                        WHERE faba.award_id = vfd.award_id
                    )
            ) AS valid_awards
            GROUP BY
                toptier_code,
                fiscal_year,
                fiscal_period
        )
        SELECT
            toptier_code,
            fiscal_year,
            fiscal_period,
            COALESCE(uc.procurement, 0) AS unlinked_procurement_c_awards,
            COALESCE(uc.assistance, 0) AS unlinked_assistance_c_awards,
            COALESCE(ud.procurement, 0) AS unlinked_d1_awards,
            COALESCE(ud.assistance, 0) AS unlinked_d2_awards,
            COALESCE(lcd.procurement, 0) AS linked_procurement_awards,
            COALESCE(lcd.assistance, 0) AS linked_assistance_awards
        FROM
            linked_file_c_and_d_awards AS lcd
        FULL OUTER JOIN
            unlinked_file_c_awards AS uc USING (toptier_code, fiscal_year, fiscal_period)
        FULL OUTER JOIN
            unlinked_file_d_awards AS ud USING (toptier_code, fiscal_year, fiscal_period)
    """,
}

CREATE_OVERVIEW_SQL = f"""
    DELETE FROM public.{OVERVIEW_TABLE_NAME};
    ALTER SEQUENCE reporting_agency_overview_reporting_agency_overview_id_seq RESTART WITH 1;

    INSERT INTO public.{OVERVIEW_TABLE_NAME} (
        fiscal_period,
        fiscal_year,
        toptier_code,
        total_dollars_obligated_gtas,
        total_budgetary_resources,
        total_diff_approp_ocpa_obligated_amounts,
        unlinked_procurement_c_awards,
        unlinked_assistance_c_awards,
        unlinked_d1_awards,
        unlinked_d2_awards,
        linked_procurement_awards,
        linked_assistance_awards
    )
    SELECT *
    FROM (
        SELECT
            fiscal_period,
            fiscal_year,
            toptier_code,
            rao.total_dollars_obligated_gtas,
            rao.total_budgetary_resources,
            rao.total_diff_approp_ocpa_obligated_amounts,
            COALESCE(ac.unlinked_procurement_c_awards, 0) AS unlinked_procurement_c_awards,
            COALESCE(ac.unlinked_assistance_c_awards, 0) AS unlinked_assistance_c_awards,
            COALESCE(ac.unlinked_d1_awards, 0) AS unlinked_d1_awards,
            COALESCE(ac.unlinked_d2_awards, 0) AS unlinked_d2_awards,
            COALESCE(ac.linked_procurement_awards, 0) AS linked_procurement_awards,
            COALESCE(ac.linked_assistance_awards, 0) AS linked_assistance_awards
        FROM
            {TempTableName.REPORTING_OVERVIEW.value} AS rao
        LEFT OUTER JOIN
            {TempTableName.AWARD_COUNTS.value} AS ac USING (toptier_code, fiscal_year, fiscal_period)
    ) AS {OVERVIEW_TABLE_NAME}_content;
"""


class Command(BaseCommand):
    """Used to calculate values and populate reporting_agency_overview"""

    cursor = None

    def handle(self, *args, **options):
        with Timer("Refresh Reporting Agency Overview"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

    def _perform_load(self):
        with connection.cursor() as cursor:
            self.cursor = cursor
            with Timer("Create temporary tables"):
                self.cursor.execute(CREATE_AND_PREP_TEMP_TABLES)

            for temp_table in TEMP_TABLE_CONTENTS:
                self._populate_temp_table(temp_table)

            with Timer("Populate Reporting Agency Overview"):
                self.cursor.execute(CREATE_OVERVIEW_SQL)

    def _populate_temp_table(self, temp_table: TempTableName) -> None:
        sql_template = "INSERT INTO {} SELECT * FROM ({}) AS {};"
        with Timer(f"Populate '{temp_table.value}'"):
            self.cursor.execute(
                sql_template.format(temp_table.value, TEMP_TABLE_CONTENTS[temp_table], f"{temp_table.value}_contents")
            )
