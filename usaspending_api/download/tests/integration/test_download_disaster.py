import json
from unittest.mock import Mock

import pytest
import re

from django.conf import settings
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.references.models import DisasterEmergencyFundCode
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

    resp = client.post("/api/v2/download/disaster/", content_type="application/json", data=json.dumps(request_body))
    return resp


@pytest.fixture
def awards_and_transactions():
    # Populate job status lookup table
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Awards
    award1 = baker.make(
        "search.AwardSearch",
        award_id=123,
        type="07",
        total_loan_value=3,
        generated_unique_award_id="ASST_NEW_1",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["L"],
    )
    award2 = baker.make(
        "search.AwardSearch",
        award_id=344,
        type="07",
        total_loan_value=30,
        generated_unique_award_id="ASST_NEW_2",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["M"],
    )
    award3 = baker.make(
        "search.AwardSearch",
        award_id=809,
        type="08",
        total_loan_value=300,
        generated_unique_award_id="ASST_NEW_3",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["N"],
    )
    award4 = baker.make(
        "search.AwardSearch",
        award_id=110,
        type="B",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_1",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["L", "M"],
    )
    award5 = baker.make(
        "search.AwardSearch",
        award_id=130,
        type="A",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_2",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["M", "N"],
    )
    award6 = baker.make(
        "search.AwardSearch",
        award_id=391,
        type="C",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_3",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["L", "N"],
    )
    award7 = baker.make(
        "search.AwardSearch",
        award_id=398,
        type="D",
        total_loan_value=0,
        generated_unique_award_id="CONT_NEW_4",
        action_date="2020-01-01",
        disaster_emergency_fund_codes=["L", "M", "N"],
    )

    # Subawards
    baker.make("search.SubawardSearch", broker_subaward_id=1, award=award1, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=2, award=award2, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=3, award=award3, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=4, award=award4, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=5, award=award5, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=6, award=award6, sub_action_date="2023-01-01")
    baker.make("search.SubawardSearch", broker_subaward_id=7, award=award7, sub_action_date="2023-01-01")

    # Disaster Emergency Fund Code
    defc1 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
    )
    defc2 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-18",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="N",
        public_law="PUBLIC LAW FOR CODE N",
        title="TITLE FOR CODE N",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-27",
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
        type="07",
        face_value_loan_guarantee=3,
        federal_action_obligation=5,
        generated_pragmatic_obligation=3,
        action_date="2022-01-01",
        fiscal_action_date="2022-04-01",
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
        type="07",
        face_value_loan_guarantee=30,
        federal_action_obligation=50,
        generated_pragmatic_obligation=30,
        action_date="2022-01-02",
        fiscal_action_date="2022-04-02",
        is_fpds=False,
        generated_unique_award_id="ASST_NEW_2",
        cfda_number="20.200",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="90",
        recipient_name="RECIPIENT 2",
        recipient_name_raw="RECIPIENT 2",
        recipient_uei=None,
        recipient_unique_id="456789123",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=30,
        award=award3,
        type="08",
        face_value_loan_guarantee=300,
        federal_action_obligation=500,
        generated_pragmatic_obligation=300,
        action_date="2022-01-03",
        fiscal_action_date="2022-04-03",
        is_fpds=False,
        generated_unique_award_id="ASST_NEW_3",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="50",
        recipient_name="RECIPIENT 3",
        recipient_name_raw="RECIPIENT 3",
        recipient_uei=None,
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=40,
        award=award4,
        type="B",
        federal_action_obligation=5000,
        generated_pragmatic_obligation=5000,
        action_date="2022-01-04",
        fiscal_action_date="2022-04-04",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_1",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="WA",
        recipient_location_state_name="WASHINGTON",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_uei=None,
        recipient_unique_id="096354360",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=50,
        award=award5,
        type="A",
        federal_action_obligation=50000,
        generated_pragmatic_obligation=50000,
        action_date="2022-01-05",
        fiscal_action_date="2022-04-05",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_2",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="WA",
        recipient_location_state_name="WASHINGTON",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
        recipient_uei=None,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=60,
        award=award6,
        type="C",
        federal_action_obligation=500000,
        generated_pragmatic_obligation=500000,
        action_date="2022-01-06",
        fiscal_action_date="2022-04-06",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_3",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
        recipient_uei=None,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=70,
        award=award7,
        type="D",
        federal_action_obligation=5000000,
        generated_pragmatic_obligation=5000000,
        action_date="2022-01-07",
        fiscal_action_date="2022-04-07",
        is_fpds=True,
        generated_unique_award_id="CONT_NEW_4",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="01",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="10",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id=None,
        recipient_uei=None,
    )

    def_codes = list(
        DisasterEmergencyFundCode.objects.filter(group_name="covid_19").order_by("code").values_list("code", flat=True)
    )
    baker.make(
        "download.DownloadJob",
        job_status_id=1,
        file_name="COVID-19_Profile_2021-09-20_H20M11S49647843.zip",
        error_message=None,
        json_request=json.dumps({"filters": {"def_codes": def_codes}}),
    )

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_csv_download_success(
    client, monkeypatch, awards_and_transactions, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = _post(client, def_codes=["L"])
    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["file_format"] == "csv"

    # "def_codes" intentionally out of order to test that the order doesn't matter
    resp = _post(client, def_codes=["M", "N", "L"])
    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_2021-09-20_H20M11S49647843.zip", resp_json["file_url"])

    resp = _post(client)
    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_2021-09-20_H20M11S49647843.zip", resp_json["file_url"])


@pytest.mark.django_db(transaction=True)
def test_tsv_download_success(
    client, monkeypatch, awards_and_transactions, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L"], file_format="tsv")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["file_format"] == "tsv"


@pytest.mark.django_db(transaction=True)
def test_pstxt_download_success(
    client, monkeypatch, awards_and_transactions, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L"], file_format="pstxt")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["file_format"] == "pstxt"


@pytest.mark.django_db(transaction=True)
def test_download_failure_with_two_defc(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"])

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.json()["detail"]
        == "The Disaster Download is currently limited to either all COVID-19 DEFC or a single COVID-19 DEFC."
    )
