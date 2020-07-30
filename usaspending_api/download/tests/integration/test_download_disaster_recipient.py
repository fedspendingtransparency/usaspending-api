import json
import pytest
import re

from model_mommy import mommy
from rest_framework import status

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
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Awards
    award1 = mommy.make("awards.Award", type="07", total_loan_value=3, generated_unique_award_id="ASST_NEW_1")
    award2 = mommy.make("awards.Award", type="07", total_loan_value=30, generated_unique_award_id="ASST_NEW_2")
    award3 = mommy.make("awards.Award", type="08", total_loan_value=300, generated_unique_award_id="ASST_NEW_3")
    award4 = mommy.make("awards.Award", type="B", total_loan_value=0, generated_unique_award_id="CONT_NEW_1")
    award5 = mommy.make("awards.Award", type="A", total_loan_value=0, generated_unique_award_id="CONT_NEW_2")
    award6 = mommy.make("awards.Award", type="C", total_loan_value=0, generated_unique_award_id="CONT_NEW_3")
    award7 = mommy.make("awards.Award", type="D", total_loan_value=0, generated_unique_award_id="CONT_NEW_4")

    # Disaster Emergency Fund Code
    defc1 = mommy.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
    )
    defc2 = mommy.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    mommy.make(
        "references.DisasterEmergencyFundCode",
        code="N",
        public_law="PUBLIC LAW FOR CODE N",
        title="TITLE FOR CODE N",
        group_name="covid_19",
    )

    # Submission Attributes
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
    )

    # Financial Accounts by Awards
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=1,
        award=award1,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=2,
        award=award2,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=10,
        transaction_obligated_amount=20,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=3,
        award=award3,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=100,
        transaction_obligated_amount=200,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=4,
        award=award4,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1000,
        transaction_obligated_amount=2000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=5,
        award=award5,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=10000,
        transaction_obligated_amount=20000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=6,
        award=award6,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=100000,
        transaction_obligated_amount=200000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=7,
        award=award7,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=1000000,
        transaction_obligated_amount=2000000,
    )

    # DABS Submission Window Schedule
    mommy.make(
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
    mommy.make(
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

    # Transaction Normalized
    mommy.make(
        "awards.TransactionNormalized",
        id=10,
        award=award1,
        federal_action_obligation=5,
        action_date="2022-01-01",
        is_fpds=False,
        unique_award_key="ASST_NEW_1",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=20,
        award=award2,
        federal_action_obligation=50,
        action_date="2022-01-02",
        is_fpds=False,
        unique_award_key="ASST_NEW_2",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=30,
        award=award3,
        federal_action_obligation=500,
        action_date="2022-01-03",
        is_fpds=False,
        unique_award_key="ASST_NEW_3",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=40,
        award=award4,
        federal_action_obligation=5000,
        action_date="2022-01-04",
        is_fpds=True,
        unique_award_key="CONT_NEW_1",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=50,
        award=award5,
        federal_action_obligation=50000,
        action_date="2022-01-05",
        is_fpds=True,
        unique_award_key="CONT_NEW_2",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=60,
        award=award6,
        federal_action_obligation=500000,
        action_date="2022-01-06",
        is_fpds=True,
        unique_award_key="CONT_NEW_3",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=70,
        award=award7,
        federal_action_obligation=5000000,
        action_date="2022-01-07",
        is_fpds=True,
        unique_award_key="CONT_NEW_4",
    )

    # Transaction FABS
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=10,
        cfda_number="10.100",
        legal_entity_country_code="USA",
        legal_entity_state_code=None,
        legal_entity_county_code=None,
        legal_entity_county_name=None,
        legal_entity_congressional=None,
        awardee_or_recipient_legal="RECIPIENT 1",
        awardee_or_recipient_uniqu=None,
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=20,
        cfda_number="20.200",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="001",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="90",
        awardee_or_recipient_legal="RECIPIENT 2",
        awardee_or_recipient_uniqu="456789123",
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=30,
        cfda_number="20.200",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="001",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="50",
        awardee_or_recipient_legal="RECIPIENT 3",
        awardee_or_recipient_uniqu="987654321",
    )

    # Transaction FPDS
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=40,
        legal_entity_country_code="USA",
        legal_entity_state_code="WA",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu="096354360",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=50,
        legal_entity_country_code="USA",
        legal_entity_state_code="WA",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal=None,
        awardee_or_recipient_uniqu="987654321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=60,
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal=None,
        awardee_or_recipient_uniqu="987654321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=70,
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="01",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="10",
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu=None,
    )

    # Recipient Profile
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 1",
        recipient_level="R",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_unique_id=None,
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 2",
        recipient_level="R",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        recipient_unique_id="456789123",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="P",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="C",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_level="R",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        recipient_unique_id="096354360",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="R",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )

    # Recipient Lookup
    mommy.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 3",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        duns="987654321",
    )

    # Set latest_award for each award
    update_awards()


def test_download_success(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"])

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "csv"


def test_tsv_download_success(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"], file_format="tsv")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "tsv"


def test_pstxt_download_success(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client, def_codes=["L", "M"], award_type_codes=["07", "08"], file_format="pstxt")

    resp_json = resp.json()
    assert resp.status_code == status.HTTP_200_OK
    assert re.match(r".*COVID-19_Profile_.*\.zip", resp_json["file_url"])
    assert resp_json["download_request"]["columns"] == [
        "recipient",
        "award_obligations",
        "award_outlays",
        "face_value_of_loans",
        "number_of_awards",
    ]
    assert resp_json["download_request"]["file_format"] == "pstxt"


def test_download_failure_with_empty_filter(client, monkeypatch, awards_and_transactions, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    resp = _post(client)

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json()["detail"] == "Missing value: 'filters|def_codes' is a required field"
