import json
from unittest.mock import Mock

import pytest
import re

from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _post(client, def_codes=None, query=None, award_type_codes=None, file_format=None):
    request_body = {}
    filters = {}

    if def_codes:
        filters["def_codes"] = def_codes
    if query:
        filters["query"] = query
    if award_type_codes:
        filters["award_type_codes"] = award_type_codes

    request_body["filters"] = filters

    if file_format:
        request_body["file_format"] = file_format

    resp = client.post(
        "/api/v2/download/disaster/recipients/", content_type="application/json", data=json.dumps(request_body)
    )
    return resp


@pytest.fixture
def awards_and_transactions(transactional_db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Awards
    award1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        type="07",
        total_loan_value=3,
        generated_unique_award_id="ASST_NEW_1",
        action_date="2020-01-01",
    )
    award2 = baker.make(
        "search.AwardSearch",
        award_id=2,
        type="07",
        total_loan_value=30,
        generated_unique_award_id="ASST_NEW_2",
        action_date="2020-01-01",
    )
    award3 = baker.make(
        "search.AwardSearch",
        award_id=3,
        type="08",
        total_loan_value=300,
        generated_unique_award_id="ASST_NEW_3",
        action_date="2020-01-01",
    )
    award4 = baker.make(
        "search.AwardSearch",
        award_id=4,
        type="B",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_1",
        action_date="2020-01-01",
    )
    award5 = baker.make(
        "search.AwardSearch",
        award_id=5,
        type="A",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_2",
        action_date="2020-01-01",
    )
    award6 = baker.make(
        "search.AwardSearch",
        award_id=6,
        type="C",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_3",
        action_date="2020-01-01",
    )
    award7 = baker.make(
        "search.AwardSearch",
        award_id=7,
        type="D",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_4",
        action_date="2020-01-01",
    )

    # Disaster Emergency Fund Code
    defc1 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
    )
    defc2 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="N",
        public_law="PUBLIC LAW FOR CODE N",
        title="TITLE FOR CODE N",
        group_name="covid_19",
    )

    # Submission Attributes
    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )
    sub2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )
    sub3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )

    # Financial Accounts by Awards
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=1,
        award=award1,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=2,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=2,
        award=award2,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=10,
        transaction_obligated_amount=20,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=3,
        award=award3,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=100,
        transaction_obligated_amount=200,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=4,
        award=award4,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1000,
        transaction_obligated_amount=2000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=5,
        award=award5,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=10000,
        transaction_obligated_amount=20000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=6,
        award=award6,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=100000,
        transaction_obligated_amount=200000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=7,
        award=award7,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=1000000,
        transaction_obligated_amount=2000000,
    )

    # DABS Submission Window Schedule
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022081",
        is_quarter=False,
        period_start_date="2022-05-01",
        period_end_date="2022-05-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022080",
        is_quarter=True,
        period_start_date="2022-05-01",
        period_end_date="2022-05-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )

    # Transaction Search
    baker.make(
        "search.TransactionSearch",
        transaction_id=10,
        award=award1,
        federal_action_obligation=5,
        action_date="2022-01-01",
        is_fpds=False,
        generated_unique_award_id="ASST_NEW_1",
        cfda_number="10.100",
        recipient_location_country_code="USA",
        recipient_location_state_code=None,
        recipient_location_county_code=None,
        recipient_location_county_name=None,
        recipient_location_congressional_code=None,
        recipient_name="RECIPIENT 1",
        recipient_name_raw="RECIPIENT 1",
        recipient_unique_id=None,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=20,
        award=award2,
        federal_action_obligation=50,
        action_date="2022-01-02",
        is_fpds=False,
        generated_unique_award_id="ASST_NEW_2",
        cfda_number="20.200",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="90",
        recipient_name="RECIPIENT 2",
        recipient_name_raw="RECIPIENT 2",
        recipient_unique_id="456789123",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=30,
        award=award3,
        federal_action_obligation=500,
        action_date="2022-01-03",
        is_fpds=False,
        generated_unique_award_id="ASST_NEW_3",
        cfda_number="20.200",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="50",
        recipient_name="RECIPIENT 3",
        recipient_name_raw="RECIPIENT 3",
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=40,
        award=award4,
        federal_action_obligation=5000,
        action_date="2022-01-04",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_1",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_unique_id="096354360",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=50,
        award=award5,
        federal_action_obligation=50000,
        action_date="2022-01-05",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_2",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=60,
        award=award6,
        federal_action_obligation=500000,
        action_date="2022-01-06",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_3",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=70,
        award=award7,
        federal_action_obligation=5000000,
        action_date="2022-01-07",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_4",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="01",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="10",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_unique_id=None,
    )

    # Recipient Profile
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 1",
        recipient_level="R",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_unique_id=None,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 2",
        recipient_level="R",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        recipient_unique_id="456789123",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="P",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="C",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_level="R",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        recipient_unique_id="096354360",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="R",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )

    # Recipient Lookup
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 3",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        duns="987654321",
    )

    # Set latest_award for each award
    update_awards()


def test_download_success(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"])

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Recipients_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "csv"


def test_tsv_download_success(
    client, monkeypatch, awards_and_transactions, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"], file_format="tsv")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Recipients_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "tsv"


def test_pstxt_download_success(
    client, monkeypatch, awards_and_transactions, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"], award_type_codes=["07", "08"], file_format="pstxt")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Recipients_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "face_value_of_loans",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "pstxt"


def test_download_failure_with_empty_filter(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client)

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json()["detail"] == "Missing value: 'filters|def_codes' is a required field"
