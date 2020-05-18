import pytest

from model_mommy import mommy

from usaspending_api.awards.models import TransactionFABS
from usaspending_api.common.recipient_lookups import obtain_recipient_uri, annotate_recipient_id
from usaspending_api.search.models import UniversalTransactionView


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


@pytest.mark.django_db
def test_annotate_recipient_id_success():
    # Test when all parameters are provided
    values = {
        "alias_name": "some_name",
        "queryset": TransactionFABS.objects,
        "id_lookup": "transaction_fabs.awardee_or_recipient_uniqu",
        "parent_id_lookup": "transaction_fabs.ultimate_parent_unique_ide",
        "name_lookup": "transaction_fabs.awardee_or_recipient_legal",
    }
    queryset = annotate_recipient_id(**values)

    expected_sql = """select
                rp.recipient_hash || '-' ||  rp.recipient_level
            from
                recipient_profile rp
                inner join recipient_lookup rl on rl.recipient_hash = rp.recipient_hash
            where
                (
                    (
                        transaction_fabs.awardee_or_recipient_uniqu is null
                        and rl.duns is null
                        and transaction_fabs.awardee_or_recipient_legal = rl.legal_business_name
                    ) or (
                        transaction_fabs.awardee_or_recipient_uniqu is not null
                        and rl.duns is not null
                        and rl.duns = transaction_fabs.awardee_or_recipient_uniqu
                    )
                )
                and rp.recipient_level = case
                    when transaction_fabs.ultimate_parent_unique_ide is null then 'R'
                    else 'C' end
                and rp.recipient_name not in ('MULTIPLE RECIPIENTS', 'REDACTED DUE TO PII', 'MULTIPLE FOREIGN RECIPIENTS', 'PRIVATE INDIVIDUAL', 'INDIVIDUAL RECIPIENT', 'MISCELLANEOUS FOREIGN AWARDEES')
        ) AS "some_name" """

    assert expected_sql in str(queryset.query)

    # Test when only required parameters are provided
    values = {"alias_name": "some_name", "queryset": UniversalTransactionView.objects}
    queryset = annotate_recipient_id(**values)

    expected_sql = """select
                rp.recipient_hash || '-' ||  rp.recipient_level
            from
                recipient_profile rp
                inner join recipient_lookup rl on rl.recipient_hash = rp.recipient_hash
            where
                (
                    (
                        "universal_transaction_matview".recipient_unique_id is null
                        and rl.duns is null
                        and "universal_transaction_matview".recipient_name = rl.legal_business_name
                    ) or (
                        "universal_transaction_matview".recipient_unique_id is not null
                        and rl.duns is not null
                        and rl.duns = "universal_transaction_matview".recipient_unique_id
                    )
                )
                and rp.recipient_level = case
                    when "universal_transaction_matview".parent_recipient_unique_id is null then 'R'
                    else 'C' end
                and rp.recipient_name not in ('MULTIPLE RECIPIENTS', 'REDACTED DUE TO PII', 'MULTIPLE FOREIGN RECIPIENTS', 'PRIVATE INDIVIDUAL', 'INDIVIDUAL RECIPIENT', 'MISCELLANEOUS FOREIGN AWARDEES')
        ) AS "some_name" """

    assert expected_sql in str(queryset.query)


@pytest.mark.django_db
def test_annotate_recipient_id_failure():
    # Test when only one optional parameter is provided
    values = {
        "alias_name": "some_name",
        "queryset": TransactionFABS.objects,
        "name_lookup": "transaction_fabs.awardee_or_recipient_legal",
    }
    try:
        annotate_recipient_id(**values)
    except Exception as e:
        assert (
            str(e) == "Invalid parameters for 'annotate_recipient_id'; requires all or just 'alias_name' and 'queryset'"
        )
    else:
        assert False, "Expected an exception to be raised"

    # Test when only two optional parameters are provided
    values = {
        "alias_name": "some_name",
        "queryset": TransactionFABS.objects,
        "id_lookup": "transaction_fabs.awardee_or_recipient_uniqu",
        "parent_id_lookup": "transaction_fabs.ultimate_parent_unique_ide",
    }
    try:
        annotate_recipient_id(**values)
    except Exception as e:
        assert (
            str(e) == "Invalid parameters for 'annotate_recipient_id'; requires all or just 'alias_name' and 'queryset'"
        )
    else:
        assert False, "Expected an exception to be raised"
