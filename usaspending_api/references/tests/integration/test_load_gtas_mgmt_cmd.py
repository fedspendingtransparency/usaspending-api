from datetime import datetime, timezone

import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.test import TestCase
from model_bakery import baker

from usaspending_api.etl.transaction_loaders.data_load_helpers import format_insert_or_update_column_sql
from usaspending_api.references.models import GTASSF133Balances


MATCHING_TAS_LABEL = "009-010-X-0100-000"


def _sf_133_row(**overrides) -> dict:
    row = {
        "fiscal_year": 2024,
        "period": 11,
        "line": 1000,
        "amount": 0,
        "disaster_emergency_fund_code": "A",
        "allocation_transfer_agency": "009",
        "agency_identifier": "010",
        "availability_type_code": "X",
        "beginning_period_of_availa": None,
        "ending_period_of_availabil": None,
        "main_account_code": "0100",
        "sub_account_code": "000",
        "budget_object_class": "000",
        "program_activity_reporting_key": "PARK01",
        "prior_year_adjustment": "X",
        "by_direct_reimbursable_fun": "D",
        "bea_category": "disc",
    }
    row.update(overrides)
    availability = (
        f"{row['beginning_period_of_availa']}/{row['ending_period_of_availabil']}"
        if row["beginning_period_of_availa"]
        else row["availability_type_code"]
    )
    ata = f"{row['allocation_transfer_agency']}-" if row["allocation_transfer_agency"] else ""
    row["tas"] = f"{ata}{row['agency_identifier']}-{availability}-{row['main_account_code']}-{row['sub_account_code']}"
    return row


def _seed_broker_sf_133(rows: list) -> None:
    """
    Insert sf_133 rows into the Broker test DB.  These writes autocommit (they are read back over a
    separate dblink connection by the loader), so clear the table first to keep each test isolated.
    """
    connection = connections[settings.BROKER_DB_ALIAS]
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM sf_133")
        for sf133_id, row in enumerate(rows, start=1):
            load_object = {"sf_133": {"sf133_id": sf133_id, **row}}
            columns, values, _ = format_insert_or_update_column_sql(cursor, load_object, "sf_133")
            cursor.execute(f"INSERT INTO sf_133 {columns} VALUES {values}")


@pytest.mark.usefixtures("broker_db_setup", "broker_server_dblink_setup")
class TestLoadGtas(TestCase):
    databases = "__all__"

    @classmethod
    def setUpTestData(cls):
        baker.make("references.DisasterEmergencyFundCode", code="A")
        baker.make("references.ProgramActivityPark", code="PARK01")
        baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label=MATCHING_TAS_LABEL)

        for year, month in [(2024, 11), (2024, 10), (2024, 9)]:
            baker.make(
                "submissions.DABSSubmissionWindowSchedule",
                submission_fiscal_year=year,
                submission_fiscal_month=month,
                is_quarter=False,
                submission_reveal_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )

    def test_full_reload(self):
        """No arguments mean truncate and reload every period, keeping only rows that match a TAS."""
        _seed_broker_sf_133(
            [
                _sf_133_row(line=1000, amount=100),
                _sf_133_row(line=1910, amount=300),
                _sf_133_row(line=3020, amount=70),
                _sf_133_row(agency_identifier="999", line=1000, amount=5),
            ]
        )

        call_command("load_gtas")

        assert GTASSF133Balances.objects.count() == 1
        row = GTASSF133Balances.objects.get()
        assert row.fiscal_year == 2024
        assert row.fiscal_period == 11
        assert row.budget_authority_unobligated_balance_brought_forward_cpe == 100
        assert row.total_budgetary_resources_cpe == 300
        assert row.gross_outlay_amount_by_tas_cpe == -70

    def test_targeted_reload_only_touches_that_period(self):
        """--fiscal-year/--fiscal-period replaces only the targeted period and leaves others alone."""
        baker.make(GTASSF133Balances, fiscal_year=2024, fiscal_period=11, tas_rendering_label=MATCHING_TAS_LABEL)
        untouched = baker.make(
            GTASSF133Balances, fiscal_year=2024, fiscal_period=10, tas_rendering_label=MATCHING_TAS_LABEL
        )

        _seed_broker_sf_133([_sf_133_row(period=11, line=1000, amount=100)])
        call_command("load_gtas", "--fiscal-year=2024", "--fiscal-period=11")

        assert GTASSF133Balances.objects.filter(id=untouched.id).exists()
        assert GTASSF133Balances.objects.filter(fiscal_year=2024, fiscal_period=10).count() == 1
        refreshed = GTASSF133Balances.objects.get(fiscal_year=2024, fiscal_period=11)
        assert refreshed.budget_authority_unobligated_balance_brought_forward_cpe == 100

    def test_incremental_reloads_only_windowed_periods(self):
        """--incremental reloads only the periods covered by the recent submission windows."""
        _seed_broker_sf_133(
            [
                _sf_133_row(period=11, line=1000, amount=100),
                _sf_133_row(period=9, line=1000, amount=50),
                _sf_133_row(period=8, line=1000, amount=25),
            ]
        )

        call_command("load_gtas", "--incremental")

        loaded_periods = set(GTASSF133Balances.objects.values_list("fiscal_period", flat=True))
        assert loaded_periods == {11, 9}
