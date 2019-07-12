import pytest

from model_mommy import mommy

from usaspending_api.common.recipient_lookups import obtain_recipient_uri


@pytest.fixture
def recipient_lookup(db):
    parent_recipient_lookup = {"duns": "123", "recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc"}
    recipient_lookup = {"duns": "456", "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367"}
    mommy.make("recipient.RecipientLookup", **parent_recipient_lookup)
    mommy.make("recipient.RecipientLookup", **recipient_lookup)


# Child Recipient Tests
@pytest.mark.django_db
def test_child_recipient_without_name_or_id(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": "123",
    }
    expected_result = None
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_name_and_no_id(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": "Child Recipient Test Without ID",
        "recipient_unique_id": None,
        "parent_recipient_unique_id": "123",
    }
    expected_result = "75c74068-3dd4-8770-52d0-999353d20f67-C"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_id_and_no_name(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": None,
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
    }
    expected_result = "f989e299-1f50-2600-f2f7-b6a45d11f367-C"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_name_and_id(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": "Child Recipient Test",
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
    }
    expected_result = "f989e299-1f50-2600-f2f7-b6a45d11f367-C"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


# Parent Recipient Tests
@pytest.mark.django_db
def test_parent_recipient_without_name_or_id(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = None
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_with_id_and_no_name(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": None,
        "recipient_unique_id": "123",
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_with_id_and_name(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": "Parent Recipient Tester",
        "recipient_unique_id": "123",
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result
