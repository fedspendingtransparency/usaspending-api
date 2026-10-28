import logging
from argparse import ArgumentParser
from typing import List, Optional, Tuple

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from usaspending_api.common.etl.postgres import mixins
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.references.models import GTASSF133Balances
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

logger = logging.getLogger("script")

DERIVED_COLUMNS = {
    "anticipated_prior_year_obligation_recoveries": [1033],
    "adjustments_to_unobligated_balance_brought_forward_fyb": [1020],
    "borrowing_authority_amount": [1340, 1440],
    "budget_authority_appropriation_amount_cpe": [1160, 1180, 1260, 1280],
    "budget_authority_unobligated_balance_brought_forward_cpe": [1000],
    "contract_authority_amount": [1540, 1640],
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": [1021, 1033],
    "obligations_incurred": [2190],
    "obligations_incurred_total_cpe": [2190],
    "other_budgetary_resources_amount_cpe": [1340, 1440, 1540, 1640, 1750, 1850],
    "prior_year_paid_obligation_recoveries": [1061],
    "spending_authority_from_offsetting_collections_amount": [1750, 1850],
    "total_budgetary_resources_cpe": [1910],
    "unobligated_balance_cpe": [2490],
    "status_of_budgetary_resources_total_cpe": [2500],
}

INVERTED_DERIVED_COLUMNS = {
    "gross_outlay_amount_by_tas_cpe": [3020],
}

# The before_year list of items is applied to records before the change_year fiscal year.
# The year_and_after list is applied to the change_year and subsequent fiscal years.
DERIVED_COLUMNS_DYNAMIC = {
    "adjustments_to_unobligated_balance_brought_forward_cpe": {
        "before_year": list(range(1010, 1043)),
        "year_and_after": list(range(1010, 1066)),
        "change_year": 2021,
    }
}
GTAS_TABLE = GTASSF133Balances.objects.model._meta.db_table

# All numeric derived columns, in the exact order they are produced by the Broker SELECT
# (simple derived, then inverted, then year-specific dynamic).  Used to keep the INSERT column
# list, the SELECT list, and the dblink typed-column definitions in lock-step.
DERIVED_COLUMN_NAMES = list(DERIVED_COLUMNS) + list(INVERTED_DERIVED_COLUMNS) + list(DERIVED_COLUMNS_DYNAMIC)

# Text columns passed straight through from the Broker.  Tuples are (Broker SELECT alias,
# destination db column) because two foreign keys land in differently named db columns.
PASSTHROUGH_COLUMNS = [
    ("disaster_emergency_fund_id", "disaster_emergency_fund_code"),
    ("tas_rendering_label", "tas_rendering_label"),
    ("budget_object_class", "budget_object_class"),
    ("program_activity_reporting_key_id", "program_activity_reporting_key_code"),
    ("prior_year_adjustment", "prior_year_adjustment"),
    ("by_direct_reimbursable_fun", "by_direct_reimbursable_fun"),
    ("bea_category", "bea_category"),
]

# Number of most-recent revealed monthly submission windows that --incremental reloads.  Closed
# periods are immutable, so this just needs to cover the window during which agencies can still
# revise a recently-submitted period.
INCREMENTAL_LOOKBACK_PERIODS = 3


class Command(mixins.ETLMixin, BaseCommand):
    help = (
        "Load GTAS SF-133 reference data from the Broker entirely in-database via dblink.  With no "
        "arguments the full table is reloaded; --incremental or --fiscal-year reload only the "
        "targeted period(s)."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--incremental",
            action="store_true",
            help="Reload only the open/recently-revealed period(s). Leaves closed periods untouched."
        )
        group.add_argument(
            "--fiscal-year", type=int, help="Target a specific year (with --fiscal-period)."
        )
        parser.add_argument("--fiscal-period", type=int)

    def handle(self, *args, **options) -> None:
        logger.info("Starting ETL script")
        targets = self._resolve_targets(options)
        self.process_data(targets)
        logger.info("GTAS ETL finished successfully!")

    def _resolve_targets(self, options: dict) -> Optional[List[Tuple[int, Optional[int]]]]:
        """
        Translate the command line options into the period(s) to reload.  Returns None for a full
        reload, or a list of (fiscal_year, fiscal_period) tuples (fiscal_period may be None to mean
        "every period in that year") to reload incrementally.
        """
        incremental = options.get("incremental")
        fiscal_year = options.get("fiscal_year")
        fiscal_period = options.get("fiscal_period")

        if fiscal_period is not None and fiscal_year is None:
            raise CommandError("--fiscal-period requires --fiscal-year.")
        if incremental and fiscal_period is not None:
            raise CommandError("--fiscal-period cannot be combined with --incremental.")

        if incremental:
            targets = self._incremental_periods()
            logger.info(f"Incremental load. Reloading period(s): {targets}")
            return targets
        if fiscal_year is not None:
            targets = [(fiscal_year, fiscal_period)]
            logger.info(f"Targeted load. Reloading period(s): {targets}")
            return targets
        logger.info("Full reload. Reloading all periods.")
        return None

    @staticmethod
    def _incremental_periods() -> List[Tuple[int, int]]:
        """The most-recent revealed monthly submission windows, newest first."""
        targets = list(
            DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=now(), is_quarter=False)
            .order_by("-submission_fiscal_year", "-submission_fiscal_month")
            .values_list("submission_fiscal_year", "submission_fiscal_month")
            .distinct()[:INCREMENTAL_LOOKBACK_PERIODS]
        )
        if not targets:
            raise CommandError("No revealed submission windows found; cannot perform an incremental GTAS load.")
        return targets

    @transaction.atomic()
    def process_data(self, targets: Optional[List[Tuple[int, Optional[int]]]] = None) -> None:
        if targets is None:
            self._execute_dml_sql(f"TRUNCATE {GTAS_TABLE}", "Truncating GTAS table")
        else:
            delete_predicate = self._period_predicate(targets, "fiscal_year", "fiscal_period")
            deleted = self._execute_dml_sql(
                f"DELETE FROM {GTAS_TABLE} WHERE {delete_predicate}",
                "Deleting GTAS records for targeted period(s)",
            )
            logger.info(f"Deleted {deleted:,} records for targeted period(s)")

        loaded = self._execute_dml_sql(self._load_sql(targets), "Loading GTAS records from Broker via dblink")
        logger.info(f"Loaded {loaded:,} records")
        logger.info("Committing transaction to database")

    @staticmethod
    def _period_predicate(
        targets: List[Tuple[int, Optional[int]]], year_column: str, period_column: str
    ) -> str:
        """Build an OR-ed predicate selecting the targeted (year, period) pairs."""
        clauses = []
        for fiscal_year, fiscal_period in targets:
            if fiscal_period is None:
                clauses.append(f"({year_column} = {fiscal_year})")
            else:
                clauses.append(f"({year_column} = {fiscal_year} AND {period_column} = {fiscal_period})")
        return " OR ".join(clauses)

    def _load_sql(self, targets: Optional[List[Tuple[int, Optional[int]]]]) -> str:
        """
        Build the single in-database statement that aggregates on the Broker (via dblink), joins to
        the matching TAS, and inserts the result. The TAS join replaces the old separate FK UPDATE
        and null-TAS DELETE: an inner join sets the FK and drops non-matching rows in one pass.
        """
        broker_predicate = ""
        if targets is not None:
            broker_predicate = f"WHERE {self._period_predicate(targets, 'sf.fiscal_year', 'sf.period')}"

        return f"""
            INSERT INTO {GTAS_TABLE} (
                {self.insert_columns}
            )
            SELECT
                {self.select_columns}
            FROM dblink(
                '{settings.BROKER_DBLINK_NAME}',
                $GTAS_BROKER${self._broker_fetch_sql(broker_predicate)}$GTAS_BROKER$
            ) AS agg (
                {self.dblink_column_definitions}
            )
            JOIN (
                SELECT DISTINCT ON (tas_rendering_label)
                    tas_rendering_label,
                    treasury_account_identifier
                FROM treasury_appropriation_account
                WHERE tas_rendering_label IS NOT NULL
                ORDER BY tas_rendering_label, treasury_account_identifier
            ) tas ON tas.tas_rendering_label = agg.tas_rendering_label
        """

    @property
    def insert_columns(self) -> str:
        columns = (
            ["fiscal_year", "fiscal_period"]
            + DERIVED_COLUMN_NAMES
            + [destination for _, destination in PASSTHROUGH_COLUMNS]
            + ["treasury_account_identifier", "create_date", "update_date"]
        )
        return ",\n                ".join(columns)

    @property
    def select_columns(self) -> str:
        columns = (
            [f"agg.{column}" for column in ["fiscal_year", "fiscal_period"] + DERIVED_COLUMN_NAMES]
            + [f"agg.{alias}" for alias, _ in PASSTHROUGH_COLUMNS]
            + ["tas.treasury_account_identifier", "now()", "now()"]
        )
        return ",\n                ".join(columns)

    @property
    def dblink_column_definitions(self) -> str:
        definitions = (
            ["fiscal_year integer", "fiscal_period integer"]
            + [f"{column} numeric(23,2)" for column in DERIVED_COLUMN_NAMES]
            + [f"{alias} text" for alias, _ in PASSTHROUGH_COLUMNS]
        )
        return ",\n                ".join(definitions)

    def _broker_fetch_sql(self, period_predicate: str = "") -> str:
        return f"""
            SELECT
                fiscal_year,
                period AS fiscal_period,
                {self.column_statements}
                disaster_emergency_fund_code AS disaster_emergency_fund_id,
                CONCAT(
                    CASE WHEN sf.allocation_transfer_agency is not null
                    THEN CONCAT(sf.allocation_transfer_agency, '-') ELSE null END,
                    sf.agency_identifier, '-',
                    CASE WHEN sf.beginning_period_of_availa is not null
                    THEN CONCAT(sf.beginning_period_of_availa, '/', sf.ending_period_of_availabil)
                    ELSE sf.availability_type_code END,
                    '-', sf.main_account_code, '-', sf.sub_account_code)
                AS tas_rendering_label,
                   budget_object_class,
                   program_activity_reporting_key as program_activity_reporting_key_id,
                   prior_year_adjustment,
                   by_direct_reimbursable_fun,
                   bea_category
            FROM
                sf_133 sf
            {period_predicate}
            GROUP BY
                fiscal_year,
                fiscal_period,
                disaster_emergency_fund_code,
                tas_rendering_label,
                budget_object_class,
                program_activity_reporting_key,
                prior_year_adjustment,
                by_direct_reimbursable_fun,
                bea_category
        """

    @property
    def column_statements(self) -> str:
        simple_fields = [
            f"""COALESCE(SUM(
                CASE WHEN line IN ({','.join([str(elem) for elem in val])})
                THEN sf.amount ELSE 0 END
            ), 0.0) AS {key},"""
            for key, val in DERIVED_COLUMNS.items()
        ]
        inverted_fields = [
            f"""COALESCE(SUM(
                CASE WHEN line IN ({','.join([str(elem) for elem in val])})
                THEN sf.amount * -1 ELSE 0 END
            ), 0.0) AS {key},"""
            for key, val in INVERTED_DERIVED_COLUMNS.items()
        ]
        year_specific_fields = [
            f"""COALESCE(SUM(CASE
                    WHEN line IN ({','.join([str(elem) for elem in val["before_year"]])})
                        AND fiscal_year < {val["change_year"]}
                    THEN sf.amount * -1
                    WHEN line IN ({','.join([str(elem) for elem in val["year_and_after"]])})
                        AND fiscal_year >= {val["change_year"]}
                    THEN sf.amount * -1
                    ELSE 0
                    END
                ), 0.0) AS {key},"""
            for key, val in DERIVED_COLUMNS_DYNAMIC.items()
        ]
        return "\n".join(simple_fields + inverted_fields + year_specific_fields)
