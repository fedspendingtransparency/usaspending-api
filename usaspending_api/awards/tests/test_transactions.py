from datetime import date
import json

from model_mommy import mommy
import pytest
from rest_framework import status

from usaspending_api.awards.models import (
    Transaction, TransactionAssistance, TransactionContract)


@pytest.mark.django_db
def test_transaction_endpoint(client):
    """Test the transaction endpoint."""

    resp = client.get('/api/v1/transactions/')
    assert resp.status_code == 200
    assert len(resp.data) > 2

    assert client.post(
        '/api/v1/transactions/?page=1&limit=4',
        content_type='application/json').status_code == 200


@pytest.mark.django_db
def test_transaction_endpoint_award_fk(client):
    """Test the transaction endpoint."""

    awd = mommy.make('awards.Award', id=10, total_obligation="2000", _fill_optional=True)
    mommy.make(
        'awards.Transaction',
        award=awd)

    assert client.post(
        '/api/v1/transactions/',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "award",
                "operation": "equals",
                "value": "10"
            }]
        })).status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_txn_total_grouped(client):
    """Test award total endpoint with a group parameter."""
    # add this test once the transaction backend is refactored
    pass


@pytest.mark.django_db
def test_txn_get_or_create():
    """Test Transaction.get_or_create method."""

    agency1 = mommy.make('references.Agency')
    agency2 = mommy.make('references.Agency')
    sub = mommy.make('submissions.SubmissionAttributes')
    awd1 = mommy.make('awards.Award', awarding_agency=agency1)
    txn1 = mommy.make(
        'awards.Transaction',
        award=awd1,
        modification_number='1',
        awarding_agency=agency1,
        submission=sub,
        last_modified_date=date(2012, 7, 13)
    )
    txn1_id = txn1.id
    assert Transaction.objects.all().count() == 1

    # record with same award but different mod number is inserted as new txn
    txn_dict = {
        'submission': sub,
        'award': awd1,
        'modification_number': '2',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2012, 3, 1)
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    assert Transaction.objects.all().count() == 2

    # record with same agency/mod # but different award is inserted as new txn
    txn_dict = {
        'submission': sub,
        'award': mommy.make('awards.Award'),
        'modification_number': '1',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2012, 3, 1)
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    assert Transaction.objects.all().count() == 3

    # record with no matching pieces of info is inserted as new txn
    txn_dict = {
        'submission': sub,
        'award': mommy.make('awards.Award'),
        'modification_number': '99',
        'awarding_agency': agency2,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2012, 3, 1)
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    assert Transaction.objects.all().count() == 4

    # if existing txn's last modified date < the incoming txn
    # last modified date, update the existing txn
    txn_dict = {
        'submission': sub,
        'award': awd1,
        'modification_number': '1',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2013, 7, 13),
        'description': 'new description'
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # expecting an update, not an insert, so txn count should be unchanged
    assert Transaction.objects.all().count() == 4
    assert txn.id == txn1_id
    assert txn.description == 'new description'
    assert txn.last_modified_date == date(2013, 7, 13)

    # if existing txn last modified date > the incoming
    # last modified date, do nothing
    txn_dict = {
        'submission': sub,
        'award': awd1,
        'modification_number': '1',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2013, 3, 1),
        'description': 'an older txn'
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # expecting an update, not an insert, so txn count should be unchanged
    assert Transaction.objects.all().count() == 4
    assert txn.description == 'new description'
    assert txn.last_modified_date == date(2013, 7, 13)

    # if the txn already exists and its certified date
    # is < the incoming certified date, update
    # the existing txn
    sub2 = mommy.make(
        'submissions.SubmissionAttributes', certified_date=date(2015, 7, 13))
    sub3 = mommy.make(
        'submissions.SubmissionAttributes', certified_date=date(2016, 7, 13))
    txn2 = mommy.make(
        'awards.Transaction',
        award=awd1,
        modification_number='5',
        awarding_agency=agency1,
        submission=sub2
    )
    txn2_id = txn2.id
    txn_dict = {
        'submission': sub3,  # same txn attributes, but use a more recent certified date
        'award': awd1,
        'modification_number': '5',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'description': 'new description'
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # there should now be one more txn in the table, and it should reflect
    # the most recent updates
    assert Transaction.objects.all().count() == 5
    assert txn.id == txn2_id
    assert txn.description == 'new description'
    assert txn.submission.certified_date == sub3.certified_date

    # if the txn already exists and its certified date is >
    # the incoming certified date, do nothing
    sub4 = mommy.make(
        'submissions.SubmissionAttributes', certified_date=date(2015, 1, 1))
    txn_dict['submission'] = sub4
    txn_dict['description'] = 'older description'
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # there should be no change in number of txn records
    assert Transaction.objects.all().count() == 5
    # attributes of txn should be unchanged
    assert txn.id == txn2_id
    assert txn.description == 'new description'

    # if the txn already exists and its certified date is < the
    # incoming last modified date, update it
    txn_dict = {
        'submission': sub,  # this submission has a null certified date
        'award': awd1,
        'modification_number': '5',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': date(2020, 2, 1),
        'description': 'even newer description'
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # there should be no change in number of txn records
    assert Transaction.objects.all().count() == 5
    # txn id should be unchanged
    assert txn.id == txn2_id
    # txn attributes should be updated
    assert txn.description == 'even newer description'
    assert txn.last_modified_date == date(2020, 2, 1)
    assert txn.submission.certified_date is None

    # if the txn alread exists and its last modified date is <
    # the incoming certified date, update it
    sub5 = mommy.make(
        'submissions.SubmissionAttributes', certified_date=date(2020, 7, 13))
    txn_dict = {
        'submission': sub5,
        'award': awd1,
        'modification_number': '5',
        'awarding_agency': agency1,
        'action_date': date(1999, 12, 31),  # irrelevant, but required txn field
        'last_modified_date': None,
        'description': 'txn from the future!'
    }
    txn = Transaction.get_or_create(**txn_dict)
    txn.save()
    # there should be no change in number of txn records
    assert Transaction.objects.all().count() == 5
    # txn id should be unchanged
    assert txn.id == txn2_id
    # txn attributes should be updated
    assert txn.description == 'txn from the future!'
    assert txn.last_modified_date is None
    assert txn.submission.certified_date == sub5.certified_date


@pytest.mark.django_db
def test_txn_assistance_get_or_create():
    """Test TransactionAssistance.get_or_create method."""

    agency1 = mommy.make('references.Agency')
    sub = mommy.make('submissions.SubmissionAttributes')
    awd1 = mommy.make('awards.Award', awarding_agency=agency1)
    txn1 = mommy.make(
        'awards.Transaction',
        award=awd1,
        modification_number='1',
        awarding_agency=agency1,
        submission=sub,
        last_modified_date=date(2012, 3, 1),
    )
    mommy.make(
        'awards.TransactionAssistance',
        transaction=txn1,
        business_funds_indicator='a',
        record_type=1,
        total_funding_amount=1000.00
    )
    assert TransactionAssistance.objects.all().count() == 1

    # an updated transaction should also update existing TranactionAssistance
    # data, not insert a new record
    ta_dict = {
        'submission': sub,
        'business_funds_indicator': 'c',
        'record_type': 2,
        'total_funding_amount': 2000,
    }
    ta2 = TransactionAssistance.get_or_create(txn1, **ta_dict)
    ta2.save()
    assert TransactionAssistance.objects.all().count() == 1
    t = Transaction.objects.get(id=txn1.id)
    assert t.assistance_data.business_funds_indicator == 'c'
    assert t.assistance_data.record_type == 2
    assert t.assistance_data.total_funding_amount == 2000

    # a new transaction gets a new TransactionAssistance record
    ta_dict = {
        'submission': sub,
        'business_funds_indicator': 'z',
        'record_type': 5,
        'total_funding_amount': 8000,
    }
    ta3 = TransactionAssistance.get_or_create(
        mommy.make('awards.Transaction'), **ta_dict)
    ta3.save()
    assert TransactionAssistance.objects.all().count() == 2


@pytest.mark.django_db
def test_txn_contract_get_or_create():
    """Test TransactionContract.get_or_create method."""

    agency1 = mommy.make('references.Agency')
    sub = mommy.make('submissions.SubmissionAttributes')
    awd1 = mommy.make('awards.Award', awarding_agency=agency1)
    txn1 = mommy.make(
        'awards.Transaction',
        award=awd1,
        modification_number='1',
        awarding_agency=agency1,
        submission=sub,
        last_modified_date=date(2012, 3, 1),
    )
    mommy.make(
        'awards.TransactionContract',
        transaction=txn1,
        piid='abc',
        potential_total_value_of_award=1000,
    )
    assert TransactionContract.objects.all().count() == 1

    # an updated transaction should also update existing TranactionContract
    # data, not insert a new record
    tc_dict = {
        'submission': sub,
        'piid': 'abc',
        'potential_total_value_of_award': 5000,
    }
    tc2 = TransactionContract.get_or_create(txn1, **tc_dict)
    tc2.save()
    assert TransactionContract.objects.all().count() == 1
    t = Transaction.objects.get(id=txn1.id)
    assert t.contract_data.piid == 'abc'
    assert t.contract_data.potential_total_value_of_award == 5000

    # a new transaction gets a new TransactionAssistance record
    tc_dict = {
        'submission': sub,
        'piid': 'xyz',
        'potential_total_value_of_award': 5555,
    }
    tc3 = TransactionContract.get_or_create(
        mommy.make('awards.Transaction'), **tc_dict)
    tc3.save()
    assert TransactionContract.objects.all().count() == 2
