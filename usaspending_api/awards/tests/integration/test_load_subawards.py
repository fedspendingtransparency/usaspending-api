import json
import pytest

from datetime import date
from django.core.management import call_command
from model_bakery import baker
from pathlib import Path
from psycopg2.extensions import AsIs
from usaspending_api.awards.management.commands.load_subawards import Command
from usaspending_api.awards.models import BrokerSubaward, Subaward
from usaspending_api.common.etl.mixins import ETLMixin
from usaspending_api.common.etl.operations import stage_table
from usaspending_api.common.helpers.sql_helpers import get_connection


SAMPLE_DATA = json.loads(Path("usaspending_api/awards/tests/data/broker_subawards.json").read_text())
MIN_ID = min([r["id"] for r in SAMPLE_DATA])
MAX_ID = max([r["id"] for r in SAMPLE_DATA])


def _stage_table_mock(source, destination, staging):
    # Insert our mock data into the database.
    insert_statement = "insert into temp_load_subawards_broker_subaward (%s) values %s"
    connection = get_connection(read_only=False)
    with connection.cursor() as cursor:
        cursor.execute("drop table if exists temp_load_subawards_broker_subaward")
        cursor.execute("create table temp_load_subawards_broker_subaward as select * from broker_subaward where 0 = 1")
        for record in SAMPLE_DATA:
            columns = record.keys()
            values = tuple(record[column] for column in columns)
            sql = cursor.cursor.mogrify(insert_statement, (AsIs(", ".join(columns)), values))
            cursor.execute(sql)
    return len(SAMPLE_DATA)


@pytest.fixture
def cursor_fixture(db, monkeypatch):
    """
    Don't attempt to make dblink calls to Broker, but allow other SQL executes to occur.
    """
    original_execute = Command._execute_function

    def _execute(self, function, timer_message, *args, **kwargs):
        if function is not stage_table:
            # Allow non-dblink calls to happen "normally".
            return original_execute(self, function, timer_message, *args, **kwargs)

        return original_execute(self, _stage_table_mock, timer_message, *args, **kwargs)

    monkeypatch.setattr(ETLMixin, "_execute_function", _execute)


def _check_data():
    subaward = Subaward.objects.get(id=3613892)

    assert subaward.id == 3613892
    assert subaward.subaward_number == "32324"
    assert subaward.amount == 20.00
    assert subaward.description == "ENGINEERING CONSULTING SERVICES"
    assert subaward.recovery_model_question1 == "false"
    assert subaward.recovery_model_question2 == "false"
    assert subaward.action_date == date(2008, 2, 10)
    assert subaward.award_report_fy_month == 7
    assert subaward.award_report_fy_year == 2010
    assert subaward.award_id is None
    assert subaward.awarding_agency_id is None
    assert subaward.cfda_id is None
    assert subaward.funding_agency_id is None
    assert subaward.award_type == "procurement"
    assert subaward.broker_award_id == 8
    assert subaward.internal_id == "ASDFASFSAFSADFSAFSDF"
    assert subaward.awarding_subtier_agency_abbreviation is None
    assert subaward.awarding_subtier_agency_name is None
    assert subaward.awarding_toptier_agency_abbreviation is None
    assert subaward.awarding_toptier_agency_name is None
    assert subaward.cfda_number is None
    assert subaward.cfda_title is None
    assert subaward.extent_competed is None
    assert subaward.fain is None
    assert subaward.funding_subtier_agency_abbreviation is None
    assert subaward.funding_subtier_agency_name is None
    assert subaward.funding_toptier_agency_abbreviation is None
    assert subaward.funding_toptier_agency_name is None
    assert subaward.last_modified_date is None
    assert subaward.latest_transaction_id is None
    assert subaward.parent_recipient_unique_id == "45545454"
    assert subaward.piid == "0000"
    assert subaward.pop_city_code is None
    assert subaward.pop_congressional_code == "25"
    assert subaward.pop_country_code == "USA"
    assert subaward.pop_country_name is None
    assert subaward.pop_county_code is None
    assert subaward.pop_county_name is None
    assert subaward.pop_state_code == "CA"
    assert subaward.pop_zip4 == "93517"
    assert subaward.prime_award_type is None
    assert subaward.prime_recipient_name == "12345 AGAIN"
    assert subaward.product_or_service_code is None
    assert subaward.product_or_service_description is None
    assert subaward.pulled_from is None
    assert subaward.recipient_location_congressional_code == "52"
    assert subaward.recipient_location_country_code == "USA"
    assert subaward.recipient_location_country_name is None
    assert subaward.recipient_location_county_code is None
    assert subaward.recipient_location_county_name is None
    assert subaward.recipient_location_state_code == "CA"
    assert subaward.recipient_location_zip5 == "92124"
    assert subaward.recipient_name == "O HAI"
    assert subaward.recipient_unique_id == "34143"
    assert subaward.type_of_contract_pricing is None
    assert subaward.type_set_aside is None
    assert subaward.pop_city_name == "SAN DIEGO"
    assert subaward.pop_state_name is None
    assert subaward.pop_street_address == "654 STREET ST"
    assert subaward.recipient_location_city_code is None
    assert subaward.recipient_location_city_name == "SAN DIEGO"
    assert subaward.dba_name is None
    assert subaward.parent_recipient_name == "TOENAIL TECHNOLOGY"
    assert subaward.business_type_code is None
    assert subaward.business_type_description == "ARCHITECTURE AND ENGINEERING (A&E),CONTRACTS,FOR-PROFIT ORGANIZATION"
    assert subaward.officer_1_amount is None
    assert subaward.officer_1_name is None
    assert subaward.officer_2_amount is None
    assert subaward.officer_2_name is None
    assert subaward.officer_3_amount is None
    assert subaward.officer_3_name is None
    assert subaward.officer_4_amount is None
    assert subaward.officer_4_name is None
    assert subaward.officer_5_amount is None
    assert subaward.officer_5_name is None
    assert subaward.recipient_location_foreign_postal_code is None
    assert subaward.recipient_location_state_name is None
    assert subaward.recipient_location_street_address == "ANOTHER ADDRESS"
    assert subaward.recipient_location_zip4 == "92124"
    assert subaward.unique_award_key == "UNIQUE AWARD KEY A"


def test_defaults(cursor_fixture):
    call_command("load_subawards")
    assert BrokerSubaward.objects.count() == 4
    assert Subaward.objects.count() == 4

    _check_data()


def test_full(cursor_fixture):
    call_command("load_subawards", "--full-reload")
    assert BrokerSubaward.objects.count() == 4
    assert Subaward.objects.count() == 4


def test_some_data_correction_conditions(cursor_fixture):
    call_command("load_subawards")
    assert BrokerSubaward.objects.count() == 4
    assert Subaward.objects.count() == 4

    # Add some garbage so we can check that everything resets after a load.
    BrokerSubaward.objects.filter(id=MIN_ID).delete()

    baker.make("awards.BrokerSubaward", id=MAX_ID + 1)
    baker.make("awards.BrokerSubaward", id=MAX_ID + 2)
    baker.make("awards.BrokerSubaward", id=MAX_ID + 3)

    baker.make("awards.Subaward", id=MAX_ID + 7)
    baker.make("awards.Subaward", id=MAX_ID + 8)
    baker.make("awards.Subaward", id=MAX_ID + 9)

    broker_subaward = BrokerSubaward.objects.get(id=3613892)
    broker_subaward.sub_recovery_model_q1 = True
    broker_subaward.save()

    broker_subaward = BrokerSubaward.objects.get(id=3613892)
    assert broker_subaward.sub_recovery_model_q1 is True

    subaward = Subaward.objects.get(id=3613892)
    subaward.recovery_model_question1 = "maybe"
    subaward.save()

    subaward = Subaward.objects.get(id=3613892)
    assert subaward.recovery_model_question1 == "maybe"

    assert BrokerSubaward.objects.count() == 6
    assert Subaward.objects.count() == 7

    # Make sure this fixes everything.
    call_command("load_subawards")
    assert BrokerSubaward.objects.count() == 4
    assert Subaward.objects.count() == 4

    _check_data()
