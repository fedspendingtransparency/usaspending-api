import pytest

from model_mommy import mommy

from usaspending_api.common.recipient_lookups import obtain_recipient_uri


@pytest.fixture
def recipient_lookup(db):
    parent_recipient_lookup = {"duns": "123", "recipient_hash": "01c03484-d1bd-41cc-2aca-4b427a2d0611"}
    recipient_lookup = {"duns": "456", "recipient_hash": "1c4e7c2a-efe3-1b7e-2190-6f4487f808ac"}
    parent_recipient_profile = {"recipient_hash": "01c03484-d1bd-41cc-2aca-4b427a2d0611", "recipient_level": "P"}
    recipient_profile = {"recipient_hash": "1c4e7c2a-efe3-1b7e-2190-6f4487f808ac", "recipient_level": "C"}
    mommy.make("recipient.RecipientLookup", **parent_recipient_lookup)
    mommy.make("recipient.RecipientLookup", **recipient_lookup)
    mommy.make("recipient.RecipientProfile", **parent_recipient_profile)
    mommy.make("recipient.RecipientProfile", **recipient_profile)

    # This is required for test_child_recipient_with_name_and_no_id.
    recipient_profile = {"recipient_hash": "b2c8fe8e-b520-c47f-31e3-3620a358ce48", "recipient_level": "C"}
    mommy.make("recipient.RecipientProfile", **recipient_profile)


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
    expected_result = "b2c8fe8e-b520-c47f-31e3-3620a358ce48-C"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_id_and_no_name(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": None,
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
    }
    expected_result = "1c4e7c2a-efe3-1b7e-2190-6f4487f808ac-C"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_name_and_id(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": "Child Recipient Test",
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
    }
    expected_result = "1c4e7c2a-efe3-1b7e-2190-6f4487f808ac-C"
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
    expected_result = "01c03484-d1bd-41cc-2aca-4b427a2d0611-P"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_with_id_and_name(recipient_lookup):
    child_recipient_parameters = {
        "recipient_name": "Parent Recipient Tester",
        "recipient_unique_id": "123",
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "01c03484-d1bd-41cc-2aca-4b427a2d0611-P"
    assert obtain_recipient_uri(**child_recipient_parameters) == expected_result
