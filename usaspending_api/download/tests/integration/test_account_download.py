import json
import pytest

from django.db import connection
from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import JOB_STATUS


@pytest.fixture
def account_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make('download.JobStatus', job_status_id=js.id, name=js.name, description=js.desc)

    # Create TreasuryAppropriationAccount models
    tas1 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-01')
    tas2 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-01')
    tas3 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-02')

    # Create AppropriationAccountBalances models
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas1)
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas2)
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas3)

    # Create FinancialAccountsByProgramActivityObjectClass models
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas1)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas2)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas3)


@pytest.mark.django_db
def test_tas_a_defaults_success(client, account_data):
    """ Test the accounts endpoint using the default filters for an account_balances file"""
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "account_balances",
                "fy": "2017",
                "quarter": "3"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_tas_b_defaults_success(client, account_data):
    """ Test the accounts endpoint using the default filters for an object_class_program_activity file"""
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "object_class_program_activity",
                "fy": "2018",
                "quarter": "1"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_tas_c_defaults_success(client, account_data):
    """ Test the accounts endpoint using the default filters for an award_financial file"""
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "award_financial",
                "fy": "2016",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_account_level_failure(client, account_data):
    """ Test the accounts endpoint with a wrong account_level """
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "not_tas_or_fa",
            "filters": {
                "submission_type": "account_balances",
                "fy": "2017",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_submission_type_failure(client, account_data):
    """ Test the accounts endpoint with a wrong submission_type """
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "not_a_b_or_c",
                "fy": "2018",
                "quarter": "2"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_fy_failure(client, account_data):
    """ Test the accounts endpoint with a wrong fiscal year (FY) """
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "award_financial",
                "fy": "string_not_int",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_quarter_failure(client, account_data):
    """ Test the accounts endpoint with a wrong quarter """
    csv_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "award_financial",
                "fy": "2017",
                "quarter": "string_not_int"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def generate_test_db_connection_string():
    db = connection.cursor().db.settings_dict
    return 'postgres://{}:{}@{}:5432/{}'.format(db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])
