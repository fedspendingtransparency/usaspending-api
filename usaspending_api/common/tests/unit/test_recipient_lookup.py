import pytest

from model_bakery import baker

from usaspending_api.common.recipient_lookups import obtain_recipient_uri


@pytest.fixture
def recipient_lookup(db):
    # DUNS Test Data
    baker.make("recipient.RecipientLookup", duns="123", uei=None, recipient_hash="01c03484-d1bd-41cc-2aca-4b427a2d0611")
    baker.make("recipient.RecipientLookup", duns="456", uei=None, recipient_hash="1c4e7c2a-efe3-1b7e-2190-6f4487f808ac")
    baker.make("recipient.RecipientProfile", recipient_hash="01c03484-d1bd-41cc-2aca-4b427a2d0611", recipient_level="P")
    baker.make("recipient.RecipientProfile", recipient_hash="1c4e7c2a-efe3-1b7e-2190-6f4487f808ac", recipient_level="C")

    # This is required for test_child_recipient_with_name_and_no_id.
    baker.make("recipient.RecipientProfile", recipient_hash="b2c8fe8e-b520-c47f-31e3-3620a358ce48", recipient_level="C")

    # UEI Test Data
    baker.make("recipient.RecipientLookup", duns=None, uei="123", recipient_hash="f5ba3b35-167d-8f32-57b0-406c3479de90")
    baker.make("recipient.RecipientLookup", duns=None, uei="456", recipient_hash="ede3440b-e344-a923-035e-feed34773b57")
    baker.make("recipient.RecipientProfile", recipient_hash="f5ba3b35-167d-8f32-57b0-406c3479de90", recipient_level="P")
    baker.make("recipient.RecipientProfile", recipient_hash="ede3440b-e344-a923-035e-feed34773b57", recipient_level="C")


# Child Recipient Tests
@pytest.mark.django_db
def test_child_recipient_without_name_or_id(recipient_lookup):
    # DUNS Test
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": None,
        "parent_recipient_uei": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": "123",
    }
    expected_result = None
    assert obtain_recipient_uri(**recipient_parameters) == expected_result

    # UEI Test
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": None,
        "parent_recipient_uei": "123",
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
    }
    expected_result = None
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_name_and_no_id(recipient_lookup):
    # Test for UEI
    recipient_parameters = {
        "recipient_name": "Child Recipient Test Without ID",
        "recipient_uei": None,
        "parent_recipient_uei": "123",
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
    }
    expected_result = "b2c8fe8e-b520-c47f-31e3-3620a358ce48-C"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_id_and_no_name(recipient_lookup):
    # Test for UEI
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": "456",
        "parent_recipient_uei": "123",
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
    }
    expected_result = "ede3440b-e344-a923-035e-feed34773b57-C"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_with_name_and_id(recipient_lookup):
    # Test for UEI
    recipient_parameters = {
        "recipient_name": "Child Recipient Test",
        "recipient_uei": "456",
        "parent_recipient_uei": "123",
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
    }
    expected_result = "ede3440b-e344-a923-035e-feed34773b57-C"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_child_recipient_preference_to_uei(recipient_lookup):
    recipient_parameters = {
        "recipient_name": "Child Recipient Test",
        "recipient_uei": "456",
        "parent_recipient_uei": "123",
        "recipient_unique_id": "NOT A REAL DUNS",  # Show that DUNS is not used when UEI is present
        "parent_recipient_unique_id": "NOT A REAL DUNS",  # Show that DUNS is not used when UEI is present
    }
    expected_result = "ede3440b-e344-a923-035e-feed34773b57-C"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


# Parent Recipient Tests
@pytest.mark.django_db
def test_parent_recipient_without_name_or_id(recipient_lookup):
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": None,
        "parent_recipient_uei": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = None
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_with_id_and_no_name(recipient_lookup):
    # Test for DUNS
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": None,
        "parent_recipient_uei": None,
        "recipient_unique_id": "123",
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "01c03484-d1bd-41cc-2aca-4b427a2d0611-P"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result

    # Test for UEI
    recipient_parameters = {
        "recipient_name": None,
        "recipient_uei": "123",
        "parent_recipient_uei": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "f5ba3b35-167d-8f32-57b0-406c3479de90-P"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_with_id_and_name(recipient_lookup):
    # Test for DUNS
    recipient_parameters = {
        "recipient_name": "Parent Recipient Tester",
        "recipient_uei": None,
        "parent_recipient_uei": None,
        "recipient_unique_id": "123",
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "01c03484-d1bd-41cc-2aca-4b427a2d0611-P"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result

    # Test for UEI
    recipient_parameters = {
        "recipient_name": "Parent Recipient Tester",
        "recipient_uei": "123",
        "parent_recipient_uei": None,
        "recipient_unique_id": None,
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "f5ba3b35-167d-8f32-57b0-406c3479de90-P"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result


@pytest.mark.django_db
def test_parent_recipient_preference_to_uei(recipient_lookup):
    # Test for DUNS
    recipient_parameters = {
        "recipient_name": "Parent Recipient Test",
        "recipient_uei": "123",
        "parent_recipient_uei": None,
        "recipient_unique_id": "NOT A REAL DUNS",  # Show that DUNS is not used when UEI is present
        "parent_recipient_unique_id": None,
        "is_parent_recipient": True,
    }
    expected_result = "f5ba3b35-167d-8f32-57b0-406c3479de90-P"
    assert obtain_recipient_uri(**recipient_parameters) == expected_result
