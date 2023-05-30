"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import json

import psycopg2
import pytz
from datetime import date, datetime
from pathlib import Path
from psycopg2.extensions import AsIs
from typing import Any, Dict, List, Optional, Union

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import fixture, mark

from django.core.management import call_command
from django.db import connection, connections, transaction, models

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple, get_database_dsn_string
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.commands.create_delta_table import (
    LOAD_QUERY_TABLE_SPEC,
    LOAD_TABLE_TABLE_SPEC,
    TABLE_SPEC,
)
from usaspending_api.etl.tests.integration.test_model import TestModel, TEST_TABLE_POSTGRES, TEST_TABLE_SPEC
from usaspending_api.recipient.models import RecipientLookup


@fixture
def populate_broker_data(broker_server_dblink_setup):
    broker_data = {
        "sam_recipient": json.loads(Path("usaspending_api/recipient/tests/data/broker_sam_recipient.json").read_text()),
        "subaward": json.loads(Path("usaspending_api/awards/tests/data/subaward.json").read_text()),
        "cd_state_grouped": json.loads(
            Path("usaspending_api/transactions/tests/data/cd_state_grouped.json").read_text()
        ),
        "zips": json.loads(Path("usaspending_api/transactions/tests/data/zips.json").read_text()),
        "cd_zips_grouped": json.loads(Path("usaspending_api/transactions/tests/data/cd_zips_grouped.json").read_text()),
        "cd_city_grouped": json.loads(Path("usaspending_api/transactions/tests/data/cd_city_grouped.json").read_text()),
        "cd_county_grouped": json.loads(
            Path("usaspending_api/transactions/tests/data/cd_county_grouped.json").read_text()
        ),
    }
    insert_statement = "INSERT INTO %(table_name)s (%(columns)s) VALUES %(values)s"
    with connections["data_broker"].cursor() as cursor:
        for table_name, rows in broker_data.items():
            # An assumption is made that each set of rows have the same columns in the same order
            columns = list(rows[0])
            values = [str(tuple(r.values())).replace("None", "null") for r in rows]
            sql_string = cursor.mogrify(
                insert_statement,
                {"table_name": AsIs(table_name), "columns": AsIs(",".join(columns)), "values": AsIs(",".join(values))},
            )
            cursor.execute(sql_string)
    yield
    # Cleanup test data for each Broker test table
    with connections["data_broker"].cursor() as cursor:
        for table in broker_data:
            cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")


@fixture
def populate_usas_data(populate_broker_data):
    # Create recipient data for two transactions; the other two will generate ad hoc
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        duns="FABSDUNS12345",
        legal_business_name="FABS TEST RECIPIENT",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientLookup",
        uei="PARENTUEI12345",
        duns="PARENTDUNS12345",
        legal_business_name="PARENT RECIPIENT 12345",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        uei="FPDSUEI12345",
        duns="FPDSDUNS12345",
        legal_business_name="FPDS RECIPIENT 12345",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        recipient_level="C",
        recipient_name="FABS TEST RECIPIENT",
        recipient_unique_id="FABSDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["PARENTUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        uei="PARENTUEI12345",
        recipient_level="P",
        recipient_name="PARENT RECIPIENT 12345",
        recipient_unique_id="PARENTDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["FABSUEI12345", "FPDSUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        uei="FPDSUEI12345",
        recipient_level="C",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_unique_id="FPDSDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["PARENTUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.DUNS",
        broker_duns_id="1",
        uei="FABSUEI12345",
        ultimate_parent_uei="PARENTUEI12345",
        ultimate_parent_unique_ide="PARENTDUNS12345",
        awardee_or_recipient_uniqu="FABSDUNS12345",
        ultimate_parent_legal_enti="PARENT RECIPIENT 12345",
        legal_business_name="FABS TEST RECIPIENT",
        _fill_optional=True,
    )

    # Create agency data
    funding_toptier_agency = baker.make(
        "references.ToptierAgency", name="TEST AGENCY 1", abbreviation="TA1", _fill_optional=True
    )
    funding_subtier_agency = baker.make(
        "references.SubtierAgency", name="TEST SUBTIER 1", abbreviation="SA1", _fill_optional=True
    )
    funding_agency = baker.make(
        "references.Agency",
        toptier_agency=funding_toptier_agency,
        subtier_agency=funding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    toptier = baker.make("references.ToptierAgency", name="toptier", abbreviation="tt", _fill_optional=True)
    subtier = baker.make("references.SubtierAgency", name="subtier", abbreviation="st", _fill_optional=True)
    agency = baker.make("references.Agency", toptier_agency=toptier, subtier_agency=subtier, toptier_flag=True, id=32)

    awarding_toptier_agency = baker.make(
        "references.ToptierAgency", name="TEST AGENCY 2", abbreviation="TA2", _fill_optional=True
    )
    awarding_subtier_agency = baker.make(
        "references.SubtierAgency", name="TEST SUBTIER 2", abbreviation="SA2", _fill_optional=True
    )
    awarding_agency = baker.make(
        "references.Agency",
        toptier_agency=awarding_toptier_agency,
        subtier_agency=awarding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    # Create reference data
    baker.make("references.NAICS", code="123456", _fill_optional=True)
    psc = baker.make("references.PSC", code="12", _fill_optional=True)
    cfda = baker.make("references.Cfda", program_number="12.456", _fill_optional=True)
    baker.make(
        "references.CityCountyStateCode",
        state_alpha="VA",
        county_numeric="001",
        county_name="County Name",
        _fill_optional=True,
    )
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES", _fill_optional=True)
    baker.make("recipient.StateData", code="VA", name="Virginia", fips="51", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="000", latest_population=1, _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="001", latest_population=1, _fill_optional=True)
    baker.make("references.PopCongressionalDistrict", state_code="51", latest_population=1, congressional_district="01")
    defc_l = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19", _fill_optional=True)
    defc_m = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19", _fill_optional=True)
    defc_q = baker.make("references.DisasterEmergencyFundCode", code="Q", group_name=None, _fill_optional=True)

    # Create account data
    federal_account = baker.make(
        "accounts.FederalAccount", parent_toptier_agency=funding_toptier_agency, _fill_optional=True
    )
    tas = baker.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=federal_account,
        allocation_transfer_agency_id=None,
        _fill_optional=True,
    )
    # Create awards and transactions
    asst_award = baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=2,
        earliest_transaction_search_id=1,
        latest_transaction_search_id=2,
        type="07",
        category="loans",
        generated_unique_award_id="UNIQUE AWARD KEY B",
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-01-01",
        certified_date="2020-04-01",
        update_date="2020-01-01",
        action_date="2020-04-01",
        fiscal_year=2020,
        award_amount=0.00,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_loan_value=0.00,
        total_obl_bin="<1M",
        type_description="Direct Loan",
        display_award_id="FAIN",
        fain="FAIN",
        uri="URI",
        piid=None,
        subaward_count=0,
        transaction_unique_id=2,
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_code=awarding_toptier_agency.toptier_code,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_code=funding_toptier_agency.toptier_code,
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_code=awarding_subtier_agency.subtier_code,
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_code=funding_subtier_agency.subtier_code,
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        funding_toptier_agency_id=funding_agency.id,
        funding_subtier_agency_id=funding_agency.id,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        cfda_number="12.456",
        cfdas=[json.dumps({"cfda_number": "12.456", "cfda_program_title": None})],
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        raw_recipient_name="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        recipient_location_state_code="VA",
        recipient_location_state_name="Virginia",
        recipient_location_state_fips=51,
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_name="Virginia",
        pop_state_fips=51,
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        disaster_emergency_fund_codes=["L", "M"],
        total_covid_outlay=0.0,
        total_covid_obligation=2.0,
        covid_spending_by_defc=[
            {"defc": "L", "outlay": 0.0, "obligation": 1.0},
            {"defc": "M", "outlay": 0.0, "obligation": 1.0},
        ],
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
    )
    cont_award = baker.make(
        "search.AwardSearch",
        award_id=2,
        type="A",
        category="contract",
        generated_unique_award_id="UNIQUE AWARD KEY C",
        latest_transaction_id=4,
        earliest_transaction_search_id=3,
        latest_transaction_search_id=4,
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-07-01",
        certified_date="2020-10-01",
        update_date="2020-01-01",
        action_date="2020-10-01",
        award_amount=0.00,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_obl_bin="<1M",
        display_award_id="PIID",
        piid="PIID",
        fain=None,
        uri=None,
        subaward_count=0,
        transaction_unique_id=2,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        raw_recipient_name="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_code=awarding_toptier_agency.toptier_code,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_code=funding_toptier_agency.toptier_code,
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_code=awarding_subtier_agency.subtier_code,
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_code=funding_subtier_agency.subtier_code,
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        funding_toptier_agency_id=funding_agency.id,
        funding_subtier_agency_id=funding_agency.id,
        recipient_location_state_code="VA",
        recipient_location_state_name="Virginia",
        recipient_location_state_fips=51,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code_current=None,
        cfdas=None,
        pop_state_code="VA",
        pop_state_name="Virginia",
        pop_state_fips=51,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        disaster_emergency_fund_codes=["Q"],
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        ordering_period_end_date="2020-07-01",
        naics_code="123456",
        product_or_service_code="12",
        product_or_service_description=psc.description,
    )
    cont_award2 = baker.make(
        "search.AwardSearch",
        award_id=3,
        generated_unique_award_id="UNIQUE AWARD KEY A",
        latest_transaction_id=434,
        earliest_transaction_search_id=434,
        latest_transaction_search_id=434,
        type="A",
        category="contract",
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-01-01",
        award_amount=0.00,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_obl_bin="<1M",
        last_modified_date="2020-01-01",
        update_date="2020-01-01",
        awarding_agency_id=32,
        funding_agency_id=32,
        awarding_toptier_agency_name=toptier.name,
        awarding_toptier_agency_name_raw="toptier",
        awarding_toptier_agency_code=toptier.toptier_code,
        funding_toptier_agency_name=toptier.name,
        funding_toptier_agency_name_raw="toptier",
        funding_toptier_agency_code=toptier.toptier_code,
        awarding_subtier_agency_name=subtier.name,
        awarding_subtier_agency_name_raw="subtier",
        awarding_subtier_agency_code=subtier.subtier_code,
        funding_subtier_agency_name=subtier.name,
        funding_subtier_agency_name_raw="subtier",
        funding_subtier_agency_code=subtier.subtier_code,
        funding_toptier_agency_id=agency.id,
        funding_subtier_agency_id=agency.id,
        display_award_id="PIID",
        piid="PIID",
        fain=None,
        uri=None,
        subaward_count=0,
        transaction_unique_id=434,
        is_fpds=True,
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        raw_recipient_name="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_congressional_code_current=None,
        pop_congressional_code_current=None,
        pop_country_code="USA",
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        treasury_account_identifiers=None,
        cfdas=None,
        tas_paths=None,
        tas_components=None,
        disaster_emergency_fund_codes=None,
        covid_spending_by_defc=None,
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        transaction_unique_id=1,
        afa_generated_unique=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        award_id=asst_award.award_id,
        award_amount=asst_award.total_subsidy_cost,
        generated_unique_award_id=asst_award.generated_unique_award_id,
        award_certified_date=asst_award.certified_date,
        award_fiscal_year=2020,
        fiscal_year=2020,
        award_date_signed=asst_award.date_signed,
        etl_update_date=asst_award.update_date,
        award_category=asst_award.category,
        piid=asst_award.piid,
        fain=asst_award.fain,
        uri=asst_award.uri,
        is_fpds=False,
        type="07",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        cfda_number="12.456",
        cfda_id=cfda.id,
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        recipient_name_raw="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=0.0,
        funding_amount=0.00,
        total_funding_amount=0.00,
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        award_update_date=asst_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["L", "M"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        transaction_unique_id=2,
        afa_generated_unique=2,
        action_date="2020-04-01",
        fiscal_action_date="2020-07-01",
        award_id=asst_award.award_id,
        award_amount=asst_award.total_subsidy_cost,
        generated_unique_award_id=asst_award.generated_unique_award_id,
        award_certified_date=asst_award.certified_date,
        award_fiscal_year=2020,
        fiscal_year=2020,
        award_date_signed=asst_award.date_signed,
        etl_update_date=asst_award.update_date,
        award_category=asst_award.category,
        piid=asst_award.piid,
        fain=asst_award.fain,
        uri=asst_award.uri,
        is_fpds=False,
        type="07",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        published_fabs_id=2,
        cfda_number="12.456",
        cfda_id=cfda.id,
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        recipient_name_raw="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        award_update_date=asst_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["L", "M"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        transaction_unique_id=3,
        detached_award_procurement_id=3,
        action_date="2020-07-01",
        fiscal_action_date="2020-10-01",
        award_id=cont_award.award_id,
        award_amount=cont_award.total_obligation,
        generated_unique_award_id=cont_award.generated_unique_award_id,
        award_certified_date=cont_award.certified_date,
        award_fiscal_year=2021,
        fiscal_year=2020,
        award_date_signed=cont_award.date_signed,
        etl_update_date=cont_award.update_date,
        award_category=cont_award.category,
        piid=cont_award.piid,
        fain=cont_award.fain,
        uri=cont_award.uri,
        is_fpds=True,
        type="A",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        naics_code="123456",
        product_or_service_code="12",
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_congressional_code_current=None,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        award_update_date=cont_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["Q"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        transaction_unique_id=4,
        detached_award_procurement_id=4,
        action_date="2020-10-01",
        fiscal_action_date="2021-01-01",
        award_id=cont_award.award_id,
        award_amount=cont_award.total_obligation,
        generated_unique_award_id=cont_award.generated_unique_award_id,
        award_certified_date=cont_award.certified_date,
        award_fiscal_year=2021,
        fiscal_year=2021,
        award_date_signed=cont_award.date_signed,
        etl_update_date=cont_award.update_date,
        award_category=cont_award.category,
        piid=cont_award.piid,
        fain=cont_award.fain,
        uri=cont_award.uri,
        is_fpds=True,
        type="A",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        naics_code="123456",
        product_or_service_code="12",
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_congressional_code_current=None,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        award_update_date=cont_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["Q"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=434,
        transaction_unique_id=434,
        detached_award_procurement_id=434,
        is_fpds=True,
        award_id=cont_award2.award_id,
        award_amount=cont_award2.total_obligation,
        generated_unique_award_id=cont_award2.generated_unique_award_id,
        award_certified_date=cont_award2.certified_date,
        etl_update_date=cont_award2.update_date,
        award_category=cont_award2.category,
        piid=cont_award2.piid,
        fain=cont_award2.fain,
        uri=cont_award2.uri,
        type="A",
        awarding_agency_id=agency.id,
        funding_agency_id=agency.id,
        awarding_toptier_agency_name=toptier.name,
        awarding_toptier_agency_name_raw="toptier",
        funding_toptier_agency_name=toptier.name,
        funding_toptier_agency_name_raw="toptier",
        awarding_subtier_agency_name=subtier.name,
        awarding_subtier_agency_name_raw="subtier",
        funding_subtier_agency_name=subtier.name,
        funding_subtier_agency_name_raw="subtier",
        awarding_toptier_agency_abbreviation=toptier.abbreviation,
        funding_toptier_agency_abbreviation=toptier.abbreviation,
        awarding_subtier_agency_abbreviation=subtier.abbreviation,
        funding_subtier_agency_abbreviation=subtier.abbreviation,
        awarding_toptier_agency_id=agency.id,
        funding_toptier_agency_id=agency.id,
        last_modified_date="2020-01-01",
        award_update_date=cont_award2.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        federal_action_obligation=0.00,
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        recipient_location_congressional_code_current=None,
        pop_congressional_code_current=None,
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=4,
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=5,
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )

    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=6,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        submission_id=33.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=7,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        submission_id=33.00,
        _fill_optional=True,
    )

    dabs = baker.make("submissions.DABSSubmissionWindowSchedule", submission_reveal_date="2020-05-01")
    sa = baker.make("submissions.SubmissionAttributes", reporting_period_start="2020-04-02", submission_window=dabs)

    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=asst_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_l,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        submission=sa,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=asst_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_m,
        submission=sa,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=cont_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_q,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        submission=sa,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=cont_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=None,
        submission=sa,
        _fill_optional=True,
    )

    # Run current Postgres ETLs to make sure data is populated_correctly
    update_awards()
    restock_duns_sql = open("usaspending_api/broker/management/sql/restock_duns.sql", "r").read()
    execute_sql_simple(restock_duns_sql.replace("VACUUM ANALYZE int.duns;", ""))
    call_command("update_recipient_lookup")
    execute_sql_simple(open("usaspending_api/recipient/management/sql/restock_recipient_profile.sql", "r").read())


def _handle_string_cast(val: str) -> Union[str, dict, list]:
    """
    JSON nested element columns are represented as JSON formatted strings in the Spark data, but nested elements in the
    Postgres data. Both columns will have a "custom_schema" defined for "<FIELD_TYPE> STRING". To handle this comparison
    the casting of values for "custom_schema" of STRING will attempt to convert a string to a nested element in the case
    of a nested element and fallback to a simple string cast.
    """
    if isinstance(val, list):
        try:
            casted = [json.loads(element) if isinstance(element, str) else element for element in val]
        except (TypeError, json.decoder.JSONDecodeError):
            casted = [str(element) for element in val]
    elif isinstance(val, dict):
        try:
            casted = {k: json.loads(element) if isinstance(element, str) else element for k, element in val.items()}
        except (TypeError, json.decoder.JSONDecodeError):
            casted = {k: str(element) for k, element in val.items()}
    else:
        try:
            casted = json.loads(val)
        except (TypeError, json.decoder.JSONDecodeError):
            casted = str(val)
    return casted


def sorted_deep(d):
    def make_tuple(v):
        if isinstance(v, list):
            return (*sorted_deep(v),)
        if isinstance(v, dict):
            return (*sorted_deep(list(v.items())),)
        return (v,)

    if isinstance(d, list):
        return sorted(map(sorted_deep, d), key=make_tuple)
    if isinstance(d, dict):
        return {k: sorted_deep(d[k]) for k in sorted(d)}
    return d


def equal_datasets(
    psql_data: List[Dict[str, Any]],
    spark_data: List[Dict[str, Any]],
    custom_schema: str,
    ignore_fields: Optional[list] = None,
):
    """Helper function to compare the two datasets. Note the column types of ds1 will be used to cast columns in ds2."""
    datasets_match = True

    # Parsing custom_schema to specify
    schema_changes = {}
    schema_type_converters = {"INT": int, "STRING": _handle_string_cast, "ARRAY<STRING>": _handle_string_cast}
    if custom_schema:
        for schema_change in custom_schema.split(", "):
            col, new_col_type = schema_change.split()[0].strip(), schema_change.split()[1].strip()
            schema_changes[col] = new_col_type

    # Iterating through the values and finding any differences
    for i, psql_row in enumerate(psql_data):
        for k, psql_val in psql_row.items():
            # Move on to the next value to check; ignoring this field
            if ignore_fields and k in ignore_fields:
                continue
            spark_val = spark_data[i][k]

            # Casting values based on the custom schema
            if (
                k.strip() in schema_changes
                and schema_changes[k].strip() in schema_type_converters
                and psql_val is not None
            ):
                spark_val = schema_type_converters[schema_changes[k].strip()](spark_val)
                psql_val = schema_type_converters[schema_changes[k].strip()](psql_val)

            # Equalize dates
            # - Postgres TIMESTAMPs may include time zones
            # - while Spark TIMESTAMPs will not
            #   - they are aligned to the Spark SQL session Time Zone (by way
            #     of spark.sql.session.timeZone conf setting) at the time they are read)
            #   - From: https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#timestamps-and-time-zones
            #   - "When writing timestamp values out to non-text data sources like Parquet, the values are just
            #      instants (like timestamp in UTC) that have no time zone information."
            if isinstance(psql_val, datetime):
                # Align to what Spark:
                #   1. Align the date to UTC, to shift the date the amount of time from the offset to UTC
                #   2. Then strip off the UTC time zone info
                utc_tz = pytz.timezone("UTC")
                psql_val_utc = psql_val.astimezone(utc_tz)
                psql_val = psql_val_utc.replace(tzinfo=None)

            # Make sure Postgres data is sorted in the case of a list since the Spark list data is sorted in ASC order
            if isinstance(psql_val, list):
                psql_val = sorted_deep(psql_val)
                if isinstance(spark_val, str):
                    spark_val = [json.loads(idx.replace("'", '"')) for idx in [spark_val]][0]
                spark_val = sorted_deep(spark_val)

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} "
                    f"left(psql):{psql_val} ({type(psql_val)}) "
                    f"right(spark):{spark_val} ({type(spark_val)})"
                )
    return datasets_match


def load_delta_table_from_postgres(
    delta_table_name: str,
    s3_bucket: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command: str = "load_table_to_delta",
):
    """Generic function that uses the create_delta_table and load_table_to_delta commands to create and load the
    given table
    """

    cmd_args = [f"--destination-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-db={alt_db}"]
    if alt_name:
        cmd_args += [f"--alt-name={alt_name}"]

    # make the table and load it
    call_command("create_delta_table", f"--spark-s3-bucket={s3_bucket}", *cmd_args)
    call_command(load_command, *cmd_args)


def verify_delta_table_loaded_to_delta(
    spark: SparkSession,
    delta_table_name: str,
    s3_bucket: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command: str = "load_table_to_delta",
    dummy_data: List[Dict[str, Any]] = None,
    ignore_fields: Optional[list] = None,
):
    """Generic function that uses the create_delta_table, load_table_to_delta, and load_query_to_delta commands to
    create and load the given table and assert it was created and loaded as expected
    """

    load_delta_table_from_postgres(delta_table_name, s3_bucket, alt_db, alt_name, load_command)

    if alt_name:
        expected_table_name = alt_name
    else:
        expected_table_name = delta_table_name.split(".")[-1]

    partition_col = TABLE_SPEC[delta_table_name].get("partition_column")
    if dummy_data is None:
        # get the postgres data to compare
        model = TABLE_SPEC[delta_table_name]["model"]
        is_from_broker = TABLE_SPEC[delta_table_name]["is_from_broker"]
        if model:
            dummy_query = model.objects
            if partition_col is not None:
                dummy_query = dummy_query.order_by(partition_col)
            dummy_data = list(dummy_query.all().values())
        elif is_from_broker:
            # model can be None if loading from the Broker
            broker_connection = connections["data_broker"]
            source_broker_name = TABLE_SPEC[delta_table_name]["source_table"]
            with broker_connection.cursor() as cursor:
                dummy_query = f"SELECT * from {source_broker_name}"
                if partition_col is not None:
                    dummy_query = f"{dummy_query} ORDER BY {partition_col}"
                cursor.execute(dummy_query)
                dummy_data = dictfetchall(cursor)
        else:
            raise ValueError(
                "No dummy data nor model provided and the table is not from the Broker. Please provide one"
                "of these for the test to compare the data."
            )

    # get the spark data to compare
    # NOTE: The ``use <db>`` from table create/load is still in effect for this verification. So no need to call again
    received_query = f"SELECT * from {expected_table_name}"
    if partition_col is not None:
        received_query = f"{received_query} ORDER BY {partition_col}"
    received_data = [row.asDict() for row in spark.sql(received_query).collect()]

    assert equal_datasets(dummy_data, received_data, TABLE_SPEC[delta_table_name]["custom_schema"], ignore_fields)


def verify_delta_table_loaded_from_delta(
    spark: SparkSession,
    delta_table_name: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command="load_table_from_delta",
    jdbc_inserts: bool = False,
    spark_s3_bucket: str = None,
    ignore_fields: Optional[list] = None,
):
    """Generic function that uses the load_table_from_delta commands to load the given table and assert it was
    downloaded as expected
    """
    cmd_args = [f"--delta-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-delta-db={alt_db}"]
    expected_table_name = delta_table_name
    if alt_name:
        cmd_args += [f"--alt-delta-name={alt_name}"]
        expected_table_name = alt_name
    if jdbc_inserts:
        cmd_args += [f"--jdbc-inserts"]
    else:
        if not spark_s3_bucket:
            raise RuntimeError(
                "spark_s3_bucket=None. A unit test S3 bucket needs to be created and provided for this test."
            )
        cmd_args += [f"--spark-s3-bucket={spark_s3_bucket}"]

    # table already made, let's load it
    call_command(load_command, *cmd_args)

    # get the postgres data to compare
    source_table = TABLE_SPEC[delta_table_name]["source_table"] or TABLE_SPEC[delta_table_name]["swap_table"]
    temp_schema = "temp"
    if source_table:
        tmp_table_name = f"{temp_schema}.{source_table}_temp"
    else:
        tmp_table_name = f"{temp_schema}.{expected_table_name}_temp"
    postgres_query = f"SELECT * FROM {tmp_table_name}"
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    if partition_col is not None:
        postgres_query = f"{postgres_query} ORDER BY {partition_col}"
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(postgres_query)
            postgres_data = dictfetchall(cursor)

    # get the spark data to compare
    delta_query = f"SELECT * FROM {expected_table_name}"
    if partition_col is not None:
        delta_query = f"{delta_query} ORDER BY {partition_col}"
    delta_data = [row.asDict() for row in spark.sql(delta_query).collect()]

    assert equal_datasets(
        postgres_data, delta_data, TABLE_SPEC[delta_table_name]["custom_schema"], ignore_fields=ignore_fields
    )


def create_and_load_all_delta_tables(spark: SparkSession, s3_bucket: str, tables_to_load: list):
    load_query_tables = [val for val in tables_to_load if val in LOAD_QUERY_TABLE_SPEC]
    load_table_tables = [val for val in tables_to_load if val in LOAD_TABLE_TABLE_SPEC]
    for dest_table in load_table_tables + load_query_tables:
        if dest_table in [
            "awards",
            "transaction_fabs",
            "transaction_normalized",
            "transaction_fpds",
            "financial_accounts_by_awards",
        ]:
            call_command(
                "create_delta_table",
                f"--destination-table={dest_table}",
                "--alt-db=int",
                f"--spark-s3-bucket={s3_bucket}",
            )
        else:
            call_command("create_delta_table", f"--destination-table={dest_table}", f"--spark-s3-bucket={s3_bucket}")

    for dest_table in load_table_tables:
        if dest_table in [
            "awards",
            "transaction_fabs",
            "transaction_normalized",
            "transaction_fpds",
            "financial_accounts_by_awards",
        ]:
            call_command(
                "load_table_to_delta",
                f"--destination-table={dest_table}",
                "--alt-db=int",
            )
        else:
            call_command(
                "load_table_to_delta",
                f"--destination-table={dest_table}",
            )

    for dest_table in load_query_tables:
        call_command("load_query_to_delta", f"--destination-table={dest_table}")

    create_ref_temp_views(spark)


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_recipient_lookup(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    ignore_fields = ["id", "update_date"]
    tables_to_load = ["sam_recipient", "transaction_fabs", "transaction_fpds", "transaction_normalized"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)

    # Test initial load of Recipient Lookup
    call_command("update_recipient_lookup")
    verify_delta_table_loaded_to_delta(
        spark,
        "recipient_lookup",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=ignore_fields,
    )

    # Create a new Transaction a transaction that represents a new name for a recipient
    new_award = baker.make(
        "search.AwardSearch",
        award_id=1000,
        type="07",
        period_of_performance_start_date="2021-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2021-01-01",
        total_obligation=100.00,
        total_subsidy_cost=100.00,
        type_description="Direct Loan",
        subaward_count=0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1001,
        afa_generated_unique=1001,
        action_date="2021-01-01",
        fiscal_action_date="2021-04-01",
        award_id=new_award.award_id,
        is_fpds=False,
        type="07",
        last_modified_date="2021-01-01",
        cfda_number="12.456",
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="ALTERNATE NAME RECIPIENT",
        recipient_name_raw="ALTERNATE NAME RECIPIENT",
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=1.0,
        total_funding_amount="2.23",
        recipient_location_state_code="VA",
        recipient_location_county_code="001",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_county_code="001",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
    )

    update_awards()

    # Test that the following load correctly merges
    call_command("update_recipient_lookup")

    # Verify that the update alternate name exists
    expected_result = ["FABS RECIPIENT 12345"]
    assert sorted(RecipientLookup.objects.filter(uei="FABSUEI12345").first().alternate_names) == expected_result

    tables_to_load = ["transaction_fabs", "transaction_normalized"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "recipient_lookup",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=ignore_fields,
    )
    verify_delta_table_loaded_from_delta(
        spark, "recipient_lookup", spark_s3_bucket=s3_unittest_data_bucket, ignore_fields=ignore_fields
    )
    verify_delta_table_loaded_from_delta(
        spark, "recipient_lookup", jdbc_inserts=True, ignore_fields=ignore_fields
    )  # test alt write strategy


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_published_fabs(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):

    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=7,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        submission_id=33.00,
        _fill_optional=True,
    )
    verify_delta_table_loaded_to_delta(spark, "published_fabs", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_recipient_profile(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "sam_recipient",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "recipient_profile", s3_unittest_data_bucket, load_command="load_query_to_delta", ignore_fields=["id"]
    )
    verify_delta_table_loaded_from_delta(spark, "recipient_profile", jdbc_inserts=True, ignore_fields=["id"])


@mark.django_db(transaction=True)
def test_load_table_to_delta_timezone_aware(spark, monkeypatch, s3_unittest_data_bucket, hive_unittest_metastore_db):
    """Test that timestamps are not inadvertently shifted due to loss of timezone during reads and writes.

    The big takeaways from this are:
    BLUF: Keep Django oriented to UTC (we're doing this) and keep the same timezone set in spark sessions where you
    are reading and writing data (we're doing this)

    1. Postgres stores its fields with data type ``timestamp with time zone`` under the hood in UTC, and it's up to
       the connection (and the ``time zone`` configuration parameter on that connection) to determine how it wants to
       LOOK AT that timezone-aware data (from what time zone perspective).
    2. All the django reads/writes are done via a UTC connection, because we use USE_TZ=True and TIME_ZONE="UTC" in
       settings.py
    3. But even if it weren't, it doesn't matter, since the data is stored in UTC in postgres regardless of what the
       settings where
    4. Where that Django conn time zone does matter, is if we naively strip off the timezone from a timezone-aware
       datetime value that was read under non-UTC settings, and assume its value is UTC-aligned. But we're luckily
       protected from any naivete like this by those settings.py settings.
    5. Spark will store Parquet data as an instant, without any timezone information. It's up to the session
       in which the data is being read to infer any timezone part on that instant. This is governed by the
       spark.sql.session.timeZone setting. If you read and write data with the same session timeZone, all is good (
       seeing it from the same perspective). But if you were to write it with one time zone and read it with another,
       it would be inadvertently adding/removing hours from the stored time instant (as it was written).
    """
    # Add another record with explict timezone on it
    tz_hst = pytz.timezone("HST")  # Hawaii–Aleutian Standard Time = UTC-10:00
    tz_utc = pytz.timezone("UTC")
    dt_naive = datetime(2022, 6, 11, 11, 11, 11)
    dt_with_tz = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_hst)
    dt_with_utc = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_utc)
    # Because if they are reflecting the same day+hour, then the HST (further east/back) TZ with a -10
    # offset is actually 10 hours ahead of the stated day+hour when looked at in the UTC timezone
    assert dt_with_utc.timestamp() < dt_with_tz.timestamp()

    # Setting up an agnostic TestModel and updating the TABLE_SPEC
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute(TEST_TABLE_POSTGRES)
    TABLE_SPEC.update(TEST_TABLE_SPEC)
    monkeypatch.setattr("usaspending_api.etl.management.commands.load_table_to_delta.TABLE_SPEC", TABLE_SPEC)

    # Prepare a model object without saving it, but do save the related fields
    # - https://model-bakery.readthedocs.io/en/latest/basic_usage.html#non-persistent-objects
    # Do this so we can save the TransactionFABS record without interference from the Django DB connections
    # Session settings (like sesssion-set time zone)
    model_with_tz = baker.prepare(
        TestModel,
        _save_related=True,
        id=3,
        test_timestamp=dt_with_tz,
    )  # type: TestModel
    populated_columns = ("id", "test_timestamp")

    def _get_sql_insert_from_model(model, populated_columns):
        values = [value for value in model._meta.local_fields if value.column in populated_columns]
        q = models.sql.InsertQuery(model)
        q.insert_values(values, [model])
        compiler = q.get_compiler("default")
        setattr(compiler, "return_id", False)
        stmts = compiler.as_sql()
        stmt = [
            stmt % tuple(f"'{param}'" if type(param) in [str, date, datetime] else param for param in params)
            for stmt, params in stmts
        ]
        return stmt[0]

    # Now save it to the test DB using a new connection, that establishes its own time zone during it session
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            fabs_insert_sql = _get_sql_insert_from_model(model_with_tz, populated_columns)
            cursor.execute(fabs_insert_sql)
            assert cursor.rowcount == 1
            new_psycopg2_conn.commit()

    # See how things look from Django's perspective
    with transaction.atomic():
        # Fetch the DB object in a new transaction
        test_model_with_tz = TestModel.objects.filter(id=3).first()
        assert test_model_with_tz is not None

        # Check that all dates are as expected
        model_datetime = test_model_with_tz.test_timestamp  # type: datetime
        assert model_datetime.tzinfo is not None

        # NOTE: this is because of our Django settings. Upon saving timezone-aware data, it shifts it to UTC
        # Specifically, in settings.py, you will find
        #   TIME_ZONE = "UTC"
        #   USE_TZ = True
        # And this value STICKS in the long-lived django ``connection`` object. So long as that is used (with the ORM
        # or with raw SQL), it will apply those time zone settings
        assert model_datetime.tzname() != "HST"
        assert model_datetime.tzname() == "UTC"
        assert model_datetime.hour == 21  # shifted +10 to counteract the UTC offset by django upon saving it
        assert model_datetime.utctimetuple().tm_hour == 21  # already shifted to UTC, so this just matches .hour (== 21)
        assert dt_naive.utctimetuple().tm_hour == dt_naive.hour  # naive, so stays the same
        assert dt_with_utc.utctimetuple().tm_hour == dt_with_utc.hour  # already UTC, so stays the same

        # Confirm also that this is the case in the DB (i.e. it was at write-time that UTC was set, not read-time
        with connection.cursor() as cursor:
            cursor.execute("select test_table.test_timestamp from test_table where id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            assert dt_from_db.tzname() == "UTC"

        # Confirm whether the Test DB created was forced to UTC timezone based on settings.py
        # (Spoiler, yes it does)
        with connection.cursor() as cursor:
            cursor.execute("show time zone")
            db_tz = [row[0] for row in cursor.fetchall()][0]
            assert db_tz is not None
            assert db_tz == "UTC"

    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            cursor.execute("select test_table.test_timestamp from test_table where id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            # Can't use traditional time zone names with tzname() since pyscopg2 uses its own time zone infos.
            # Use psycopg2 tzinfo name and then compare their delta
            assert dt_from_db.tzname() == "-10"
            assert dt_from_db.utcoffset().total_seconds() == -36000.0

    # Now with that DB data committed, and with the DB set to HST TIME ZONE, do a Spark read
    try:
        # Hijack the spark time zone setting for the purposes of this test to make it NOT UTC
        # This should not matter so long as the data is WRITTEN (e.g. to parquet)
        # and READ (e.g. from a Delta Table over that parquet into a DataFrame) under the SAME timezone
        original_spark_tz = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        verify_delta_table_loaded_to_delta(spark, "test_table", s3_unittest_data_bucket)
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_spark_tz)
        with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
            with new_psycopg2_conn.cursor() as cursor:
                cursor.execute("DROP TABLE test_table")


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_detached_award_procurement(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id="4",
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id="5",
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )

    verify_delta_table_loaded_to_delta(spark, "detached_award_procurement", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
@mark.skip(reason="Due to the nature of the views with all the transformations, this will be out of date")
def test_load_table_to_from_delta_for_recipient_profile_testing(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "recipient_lookup",
        "sam_recipient",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "recipient_profile_testing", s3_unittest_data_bucket, load_command="load_table_to_delta"
    )


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_search",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=["award_update_date", "etl_update_date"],
    )
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_from_delta(spark, "transaction_search", spark_s3_bucket=s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(spark, "transaction_search", jdbc_inserts=True)  # test alt write strategy


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search_testing(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_to_delta(spark, "transaction_search_testing", s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(spark, "transaction_search_testing", spark_s3_bucket=s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(
    #     spark, "transaction_search_testing", jdbc_inserts=True
    # )  # test alt write strategy
    pass


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_normalized_alt_db_and_name(
    spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):
    baker.make("search.TransactionSearch", transaction_id="1", award_id=1, _fill_optional=True)
    baker.make("search.TransactionSearch", transaction_id="2", award_id=2, _fill_optional=True)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_normalized",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_normalized_alt_name",
    )


@mark.django_db(transaction=True)
@mark.skip(reason="Due to the nature of the views with all the transformations, this will be out of date")
def test_load_table_to_from_delta_for_transaction_search_alt_db_and_name(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_search",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_search_alt_name",
        load_command="load_query_to_delta",
    )
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_from_delta(
    #     spark,
    #     "transaction_search",
    #     alt_db="my_alt_db",
    #     alt_name="transaction_search_alt_name",
    #     spark_s3_bucket=s3_unittest_data_bucket,
    # )


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_award_search(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "award_search", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    verify_delta_table_loaded_from_delta(spark, "award_search", spark_s3_bucket=s3_unittest_data_bucket)
    verify_delta_table_loaded_from_delta(spark, "award_search", jdbc_inserts=True)  # test alt write strategy


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_sam_recipient(spark, s3_unittest_data_bucket, populate_broker_data):
    expected_data = [
        {
            "awardee_or_recipient_uniqu": "812918241",
            "legal_business_name": "EL COLEGIO DE LA FRONTERA SUR",
            "dba_name": "RESEARCH CENTER",
            "ultimate_parent_unique_ide": "811979236",
            "ultimate_parent_legal_enti": "GOBIERNO FEDERAL DE LOS ESTADOS UNIDOS MEXICANOS",
            "address_line_1": "CALLE 10 NO. 264, ENTRE 61 Y 63",
            "address_line_2": "",
            "city": "CAMPECHE",
            "state": "CAMPECHE",
            "zip": "24000",
            "zip4": None,
            "country_code": "MEX",
            "congressional_district": None,
            "business_types_codes": ["20", "2U", "GW", "M8", "V2"],
            "entity_structure": "X6",
            "broker_duns_id": "1",
            "update_date": date(2015, 2, 5),
            "uei": "CTKJDNGYLM97",
            "ultimate_parent_uei": "KDULNMSMR7E6",
        }
    ]
    verify_delta_table_loaded_to_delta(
        spark, "sam_recipient", s3_unittest_data_bucket, load_command="load_query_to_delta", dummy_data=expected_data
    )


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_summary_state_view(
    spark, s3_unittest_data_bucket, populate_usas_data, hive_unittest_metastore_db
):
    tables_to_load = [
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "summary_state_view", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
