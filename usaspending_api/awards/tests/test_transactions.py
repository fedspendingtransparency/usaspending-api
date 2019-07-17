from datetime import date
import json

from model_mommy import mommy
import pytest
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS


@pytest.mark.django_db
def test_transaction_endpoint_v1(client):
    """Test the transaction endpoint."""

    resp = client.get("/api/v1/transactions/")
    assert resp.status_code == 200
    assert len(resp.data) > 1

    assert client.post("/api/v1/transactions/?page=1&limit=4", content_type="application/json").status_code == 200


@pytest.mark.django_db
def test_transaction_endpoint_v1_award_fk(client):
    """Test the transaction endpoint."""

    awd = mommy.make("awards.Award", id=10, total_obligation="2000", _fill_optional=True)
    mommy.make("awards.TransactionNormalized", award=awd)

    assert (
        client.post(
            "/api/v1/transactions/",
            content_type="application/json",
            data=json.dumps({"filters": [{"field": "award", "operation": "equals", "value": "10"}]}),
        ).status_code
        == status.HTTP_200_OK
    )


@pytest.mark.django_db
def test_transaction_endpoint_v2(client):
    """Test the transaction endpoint."""

    resp = client.post("/api/v2/transactions/", {"award_id": "1", "page": "1", "limit": "10", "order": "asc"})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 1


@pytest.mark.django_db
def test_transaction_endpoint_v2_award_fk(client):
    """Test the transaction endpoint."""

    awd = mommy.make(
        "awards.Award", id=10, total_obligation="2000", _fill_optional=True, generated_unique_award_id="-TEST-"
    )
    mommy.make("awards.TransactionNormalized", description="this should match", _fill_optional=True, award=awd)

    resp = client.post("/api/v2/transactions/", {"award_id": "10"})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"

    resp = client.post("/api/v2/transactions/", {"award_id": "-TEST-"})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"


@pytest.mark.django_db
def test_txn_total_grouped(client):
    """Test award total endpoint with a group parameter."""
    # add this test once the transaction backend is refactored
    pass


@pytest.mark.django_db
def test_txn_get_or_create():
    """Test TransactionNormalized.get_or_create_transaction method."""

    agency1 = mommy.make("references.Agency")
    agency2 = mommy.make("references.Agency")
    awd1 = mommy.make("awards.Award", awarding_agency=agency1)
    txn1 = mommy.make(
        "awards.TransactionNormalized",
        award=awd1,
        modification_number="1",
        awarding_agency=agency1,
        last_modified_date=date(2012, 7, 13),
    )
    txn1_id = txn1.id
    assert TransactionNormalized.objects.all().count() == 1

    # record with same award but different mod number is inserted as new txn
    txn_dict = {
        "award": awd1,
        "modification_number": "2",
        "awarding_agency": agency1,
        "action_date": date(1999, 12, 31),  # irrelevant, but required txn field
        "last_modified_date": date(2012, 3, 1),
    }
    txn = TransactionNormalized.get_or_create_transaction(**txn_dict)
    txn.save()
    assert TransactionNormalized.objects.all().count() == 2

    # record with same agency/mod # but different award is inserted as new txn
    txn_dict = {
        "award": mommy.make("awards.Award"),
        "modification_number": "1",
        "awarding_agency": agency1,
        "action_date": date(1999, 12, 31),  # irrelevant, but required txn field
        "last_modified_date": date(2012, 3, 1),
    }
    txn = TransactionNormalized.get_or_create_transaction(**txn_dict)
    txn.save()
    assert TransactionNormalized.objects.all().count() == 3

    # record with no matching pieces of info is inserted as new txn
    txn_dict = {
        "award": mommy.make("awards.Award"),
        "modification_number": "99",
        "awarding_agency": agency2,
        "action_date": date(1999, 12, 31),  # irrelevant, but required txn field
        "last_modified_date": date(2012, 3, 1),
    }
    txn = TransactionNormalized.get_or_create_transaction(**txn_dict)
    txn.save()
    assert TransactionNormalized.objects.all().count() == 4

    # if existing txn's last modified date < the incoming txn
    # last modified date, update the existing txn
    txn_dict = {
        "award": awd1,
        "modification_number": "1",
        "awarding_agency": agency1,
        "action_date": date(1999, 12, 31),  # irrelevant, but required txn field
        "last_modified_date": date(2013, 7, 13),
        "description": "new description",
    }
    txn = TransactionNormalized.get_or_create_transaction(**txn_dict)
    txn.save()
    # expecting an update, not an insert, so txn count should be unchanged
    assert TransactionNormalized.objects.all().count() == 4
    assert txn.id == txn1_id
    assert txn.description == "new description"
    assert txn.last_modified_date == date(2013, 7, 13)

    # if existing txn last modified date > the incoming
    # last modified date, do nothing
    txn_dict = {
        "award": awd1,
        "modification_number": "1",
        "awarding_agency": agency1,
        "action_date": date(1999, 12, 31),  # irrelevant, but required txn field
        "last_modified_date": date(2013, 3, 1),
        "description": "an older txn",
    }
    txn = TransactionNormalized.get_or_create_transaction(**txn_dict)
    txn.save()
    # expecting an update, not an insert, so txn count should be unchanged
    assert TransactionNormalized.objects.all().count() == 4
    assert txn.description == "new description"
    assert txn.last_modified_date == date(2013, 7, 13)


@pytest.mark.django_db
def test_txn_assistance_get_or_create():
    """Test TransactionFABS.get_or_create_2 method."""

    agency1 = mommy.make("references.Agency")
    awd1 = mommy.make("awards.Award", awarding_agency=agency1)
    txn1 = mommy.make(
        "awards.TransactionNormalized",
        award=awd1,
        modification_number="1",
        awarding_agency=agency1,
        last_modified_date=date(2012, 3, 1),
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction=txn1,
        business_funds_indicator="a",
        record_type=1,
        total_funding_amount=1000.00,
    )
    assert TransactionFABS.objects.all().count() == 1

    # an updated transaction should also update existing TranactionAssistance
    # data, not insert a new record
    ta_dict = {"business_funds_indicator": "c", "record_type": 2, "total_funding_amount": 2000}
    ta2 = TransactionFABS.get_or_create_2(txn1, **ta_dict)
    ta2.save()
    assert TransactionFABS.objects.all().count() == 1
    t = TransactionNormalized.objects.get(id=txn1.id)
    assert t.assistance_data.business_funds_indicator == "c"
    assert t.assistance_data.record_type == 2
    assert t.assistance_data.total_funding_amount == "2000"

    # a new transaction gets a new TransactionFABS record
    ta_dict = {"business_funds_indicator": "z", "record_type": 5, "total_funding_amount": 8000}
    ta3 = TransactionFABS.get_or_create_2(mommy.make("awards.TransactionNormalized"), **ta_dict)
    ta3.save()
    assert TransactionFABS.objects.all().count() == 2


@pytest.mark.django_db
def test_txn_contract_get_or_create():
    """Test TransactionFPDS.get_or_create_2 method."""

    agency1 = mommy.make("references.Agency")
    awd1 = mommy.make("awards.Award", awarding_agency=agency1)
    txn1 = mommy.make(
        "awards.TransactionNormalized",
        award=awd1,
        modification_number="1",
        awarding_agency=agency1,
        last_modified_date=date(2012, 3, 1),
    )
    mommy.make("awards.TransactionFPDS", transaction=txn1, piid="abc", base_and_all_options_value=1000)
    assert TransactionFPDS.objects.all().count() == 1

    # an updated transaction should also update existing TransactionFPDS
    # data, not insert a new record
    tc_dict = {"piid": "abc", "base_and_all_options_value": 5000}
    tc2 = TransactionFPDS.get_or_create_2(txn1, **tc_dict)
    tc2.save()
    assert TransactionFPDS.objects.all().count() == 1
    t = TransactionNormalized.objects.get(id=txn1.id)
    assert t.contract_data.piid == "abc"
    assert t.contract_data.base_and_all_options_value == "5000"

    # a new transaction gets a new TransactionFABS record
    tc_dict = {"piid": "xyz", "base_and_all_options_value": 5555}
    tc3 = TransactionFPDS.get_or_create_2(mommy.make("awards.TransactionNormalized"), **tc_dict)
    tc3.save()
    assert TransactionFPDS.objects.all().count() == 2
