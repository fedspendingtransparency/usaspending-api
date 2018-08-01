# Stdlib imports
import datetime
from collections import OrderedDict

# Core Django imports

# Third-party app imports
import pytest
from rest_framework import status
from model_mommy import mommy
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.recipient.v2.views import recipients
from usaspending_api.recipient.models import RecipientProfile, DUNS
from usaspending_api.references.models import RefCountryCode, Location, LegalEntity

# Getting relative dates as the 'latest'/default argument returns results relative to when it gets called
TODAY = datetime.datetime.now()
INSIDE_OF_LATEST = (TODAY - datetime.timedelta(365 - 2))

TEST_REF_COUNTRY_CODE = {
    'PARENT COUNTRY CODE': {
        'country_code': 'PARENT COUNTRY CODE',
        'country_name': 'PARENT COUNTRY NAME'
    },
    'CHILD COUNTRY CODE': {
        'country_code': 'CHILD COUNTRY CODE',
        'country_name': 'CHILD COUNTRY NAME'
    }
}
TEST_LOCATIONS = {
    '00077a9a-5a70-8919-fd19-330762af6b84': {
        'address_line1': 'PARENT ADDRESS LINE 1',
        'address_line2': 'PARENT ADDRESS LINE 2',
        'address_line3': 'PARENT ADDRESS LINE 3',
        'county_name': 'PARENT COUNTY',
        'city_name': 'PARENT CITY',
        'congressional_code': 'PARENT CONGRESSIONAL DISTRICT',
        'location_country_code': 'PARENT COUNTRY CODE',
        'country_name': 'PARENT COUNTRY NAME',
        'state_code': 'PARENT STATE',
        'zip4': 'PARENT ZIP',
        'zip_4a': 'PARENT ZIP4',
        'foreign_province': 'PARENT FOREIGN PROVINCE',
        'foreign_postal_code': 'PARENT FOREIGN POSTAL CODE'
    },
    '00002940-fdbe-3fc5-9252-d46c0ae8758c': {
        'address_line1': 'CHILD ADDRESS LINE 1',
        'address_line2': 'CHILD ADDRESS LINE 2',
        'address_line3': 'CHILD ADDRESS LINE 3',
        'county_name': 'CHILD COUNTY',
        'city_name': 'CHILD CITY',
        'congressional_code': 'CHILD CONGRESSIONAL DISTRICT',
        'location_country_code': 'CHILD COUNTRY CODE',
        'country_name': 'CHILD COUNTRY NAME',
        'state_code': 'CHILD STATE',
        'zip4': 'CHILD ZIP',
        'zip_4a': 'CHILD ZIP4',
        'foreign_province': 'CHILD FOREIGN PROVINCE',
        'foreign_postal_code': 'CHILD FOREIGN POSTAL CODE'
    }
}
MAP_DUNS_TO_CONTRACT = {
    'address_line_1': 'address_line1',
    'address_line_2': 'address_line2',
    'city': 'city_name',
    'congressional_district': 'congressional_code',
    'state': 'state_code'
}
MAP_LOCATION_TO_CONTRACT = OrderedDict([
    ('zip4', 'zip'),
    ('zip_4a', 'zip4'),
    ('location_country_code', 'country_code')
])

TEST_DUNS = {
    '000000001': {
        'awardee_or_recipient_uniqu': '000000001',
        'legal_business_name': 'PARENT RECIPIENT',
        'address_line_1': 'PARENT ADDRESS LINE 1',
        'address_line_2': 'PARENT ADDRESS LINE 2',
        'city': 'PARENT CITY',
        'congressional_district': 'PARENT CONGRESSIONAL DISTRICT',
        'country_code': 'PARENT COUNTRY CODE',
        'state': 'PARENT STATE',
        'zip': 'PARENT ZIP',
        'zip4': 'PARENT ZIP4',
        'business_types_codes': ['PARENT BUSINESS TYPES CODES']
    },
    '000000002': {
        'awardee_or_recipient_uniqu': '000000002',
        'legal_business_name': 'CHILD RECIPIENT',
        'address_line_1': 'CHILD ADDRESS LINE 1',
        'address_line_2': 'CHILD ADDRESS LINE 2',
        'city': 'CHILD CITY',
        'congressional_district': 'CHILD CONGRESSIONAL DISTRICT',
        'country_code': 'CHILD COUNTRY CODE',
        'state': 'CHILD STATE',
        'zip': 'CHILD ZIP',
        'zip4': 'CHILD ZIP4',
        'business_types_codes': ['CHILD BUSINESS TYPES CODES']
    }
}
TEST_RECIPIENT_LOOKUPS = {
    '00077a9a-5a70-8919-fd19-330762af6b84': {
        'recipient_hash': '00077a9a-5a70-8919-fd19-330762af6b84',
        'duns': '000000001',
        'legal_business_name': 'PARENT RECIPIENT'
    },
    '392052ae-92ab-f3f4-d9fa-b57f45b7750b': {
        'recipient_hash': '392052ae-92ab-f3f4-d9fa-b57f45b7750b',
        'duns': '000000002',
        'legal_business_name': 'CHILD RECIPIENT'
    },
    '00002940-fdbe-3fc5-9252-d46c0ae8758c': {
        'recipient_hash': '00002940-fdbe-3fc5-9252-d46c0ae8758c',
        'duns': None,
        'legal_business_name': 'OTHER RECIPIENT'
    }
}
TEST_RECIPIENT_PROFILES = {
    # Parent Recipient, including non-existent child duns
    '00077a9a-5a70-8919-fd19-330762af6b84-P': {
        'recipient_level': 'P',
        'recipient_hash': '00077a9a-5a70-8919-fd19-330762af6b84',
        'recipient_unique_id': '000000001',
        'recipient_name': 'PARENT RECIPIENT',
        'recipient_affiliations': ['000000001', '000000002', '000000005']
    },
    # Child Recipient 1 - lists itself as both parent and child
    '00077a9a-5a70-8919-fd19-330762af6b84-C': {
        'recipient_level': 'C',
        'recipient_hash': '00077a9a-5a70-8919-fd19-330762af6b84',
        'recipient_unique_id': '000000001',
        'recipient_name': 'PARENT RECIPIENT',
        'recipient_affiliations': ['000000001']
    },
    # Child Recipient 2 - different from parent duns
    '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C': {
        'recipient_level': 'C',
        'recipient_hash': '392052ae-92ab-f3f4-d9fa-b57f45b7750b',
        'recipient_unique_id': '000000002',
        'recipient_name': 'CHILD RECIPIENT',
        'recipient_affiliations': ['000000001']
    },
    # Other Recipient
    '00002940-fdbe-3fc5-9252-d46c0ae8758c-R': {
        'recipient_level': 'R',
        'recipient_hash': '00002940-fdbe-3fc5-9252-d46c0ae8758c',
        'recipient_unique_id': None,
        'recipient_name': 'OTHER RECIPIENT',
        'recipient_affiliations': []
    }
}
TEST_UNIVERSAL_TRANSACTIONS = {
    'latest': {
        'transaction_id': '1',
        'action_date': INSIDE_OF_LATEST,
        'generated_pragmatic_obligation': 100
    },
    'FY2016': {
        'transaction_id': '2',
        'action_date': datetime.datetime(2015, 10, 1),
        'generated_pragmatic_obligation': 50
    },
    'FY2008': {
        'transaction_id': '3',
        'action_date': datetime.datetime(2007, 10, 1),
        'generated_pragmatic_obligation': 200
    }
}


@pytest.mark.django_db
def test_validate_recipient_id_success():
    """ Testing a run of a valid recipient id """
    recipient_id = '00077a9a-5a70-8919-fd19-330762af6b84-P'
    mommy.make(RecipientProfile, **TEST_RECIPIENT_PROFILES[recipient_id])

    expected_hash = recipient_id[:-2]
    expected_level = recipient_id[-1]
    try:
        recipient_hash, recipient_level = recipients.validate_recipient_id(recipient_id)
        assert recipient_hash == expected_hash
        assert recipient_level == expected_level
    except InvalidParameterException:
        assert False


@pytest.mark.django_db
def test_validate_recipient_id_failures():
    """ Testing a run of invalid recipient ids """
    recipient_id = '00077a9a-5a70-8919-fd19-330762af6b84-P'
    mommy.make(RecipientProfile, **TEST_RECIPIENT_PROFILES[recipient_id])

    def call_validate_recipient_id(recipient_id):
        try:
            recipients.validate_recipient_id(recipient_id)
            return False
        except InvalidParameterException:
            return True

    # Test with no dashes
    recipient_id = 'broken_recipient_id'
    assert call_validate_recipient_id(recipient_id) is True

    # Test with invalid recipient level
    recipient_id = 'broken_recipient-id'
    assert call_validate_recipient_id(recipient_id) is True

    # Test with invalid hash
    recipient_id = 'broken_recipient-R'
    assert call_validate_recipient_id(recipient_id) is True

    # Test with id not available
    recipient_id = '00002940-fdbe-3fc5-9252-000000-R'
    assert call_validate_recipient_id(recipient_id) is True


@pytest.mark.django_db
def test_extract_name_duns_from_hash(mock_reference_matviews):
    """ Testing extracting name and duns from the recipient hash """
    recipient_hash = '00077a9a-5a70-8919-fd19-330762af6b84'
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[recipient_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])

    expected_name = TEST_RECIPIENT_LOOKUPS[recipient_hash]['legal_business_name']
    expected_duns = TEST_RECIPIENT_LOOKUPS[recipient_hash]['duns']
    duns, name = recipients.extract_name_duns_from_hash(recipient_hash)
    assert duns == expected_duns
    assert name == expected_name


@pytest.mark.django_db
def test_extract_parent_from_hash(mock_reference_matviews):
    """ Testing extracting parent duns/name from recipient hash"""
    # This one specifically has to be a child
    recipient_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    recipient_hash = TEST_RECIPIENT_PROFILES[recipient_id]['recipient_hash']
    parent_duns = TEST_RECIPIENT_PROFILES[recipient_id]['recipient_affiliations'][0]
    mommy.make(RecipientProfile, **TEST_RECIPIENT_PROFILES[recipient_id])

    expected_parent_id = '00077a9a-5a70-8919-fd19-330762af6b84-P'
    parent_hash = expected_parent_id[:-2]
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[parent_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])

    test_duns_model = TEST_DUNS[parent_duns]
    mommy.make(DUNS, **test_duns_model)

    expected_name = test_duns_model['legal_business_name']
    expected_duns = parent_duns
    parent_duns, parent_name, parent_id = recipients.extract_parent_from_hash(recipient_hash)
    assert parent_duns == expected_duns
    assert parent_name == expected_name
    assert parent_id == expected_parent_id


@pytest.mark.django_db
def test_extract_parent_from_hash_failure(mock_reference_matviews):
    """ Testing extracting parent duns/name from recipient hash but with recipient lookup removed
        as there may be cases where the parent recipient is not found/listed
    """
    # This one specifically has to be a child
    recipient_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    recipient_hash = TEST_RECIPIENT_PROFILES[recipient_id]['recipient_hash']
    parent_duns = TEST_RECIPIENT_PROFILES[recipient_id]['recipient_affiliations'][0]
    mommy.make(RecipientProfile, **TEST_RECIPIENT_PROFILES[recipient_id])

    #

    test_duns_model = TEST_DUNS[parent_duns]
    mommy.make(DUNS, **test_duns_model)

    expected_name = None
    expected_duns = parent_duns
    expected_parent_id = None
    parent_duns, parent_name, parent_id = recipients.extract_parent_from_hash(recipient_hash)
    assert parent_duns == expected_duns
    assert parent_name == expected_name
    assert parent_id == expected_parent_id


@pytest.mark.django_db
def test_extract_location_duns(mock_reference_matviews):
    """ Testing extracting location data from recipient hash using the DUNS table """
    recipient_hash = '00077a9a-5a70-8919-fd19-330762af6b84'
    recipient_duns = TEST_RECIPIENT_LOOKUPS[recipient_hash]['duns']
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[recipient_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])
    test_duns_model = TEST_DUNS[recipient_duns]
    country_code = test_duns_model['country_code']
    mommy.make(DUNS, **test_duns_model)
    mommy.make(RefCountryCode, **TEST_REF_COUNTRY_CODE[country_code])

    non_location_fields = ['awardee_or_recipient_uniqu', 'legal_business_name']
    additional_blank_fields = ['address_line3', 'foreign_province', 'county_name', 'foreign_postal_code']
    expected_location = test_duns_model.copy()
    expected_location.pop('business_types_codes')
    for non_location_field in non_location_fields:
        del expected_location[non_location_field]
    expected_location['country_name'] = TEST_REF_COUNTRY_CODE[country_code]['country_name']
    for additional_blank_field in additional_blank_fields:
        expected_location[additional_blank_field] = None
    for k in MAP_DUNS_TO_CONTRACT:
        expected_location[MAP_DUNS_TO_CONTRACT[k]] = expected_location[k]
        del expected_location[k]
    location = recipients.extract_location(recipient_hash)
    assert location == expected_location


@pytest.mark.django_db
def test_extract_location_le(mock_reference_matviews):
    """ Testing extracting location data from recipient hash using the Legal Entity table """
    recipient_hash = '00077a9a-5a70-8919-fd19-330762af6b84'
    recipient_name = TEST_RECIPIENT_LOOKUPS[recipient_hash]['legal_business_name']
    recipient_duns = TEST_RECIPIENT_LOOKUPS[recipient_hash]['duns']
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[recipient_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])
    location = TEST_LOCATIONS[recipient_hash]
    mock_location = mommy.make(Location, **TEST_LOCATIONS[recipient_hash])
    mommy.make(LegalEntity, location=mock_location, recipient_name=recipient_name, recipient_unique_id=recipient_duns)

    expected_location = location.copy()
    for k in MAP_LOCATION_TO_CONTRACT:
        expected_location[MAP_LOCATION_TO_CONTRACT[k]] = expected_location[k]
        del expected_location[k]
    location = recipients.extract_location(recipient_hash)
    assert location == expected_location


@pytest.mark.django_db
def test_extract_business_categories(mock_reference_matviews):
    """ Testing extracting business categories from the recipient name/duns """
    recipient_hash = '00077a9a-5a70-8919-fd19-330762af6b84'
    recipient_name = TEST_RECIPIENT_LOOKUPS[recipient_hash]['legal_business_name']
    recipient_duns = TEST_RECIPIENT_LOOKUPS[recipient_hash]['duns']
    expected_business_cat = ['expected', 'business', 'cat']
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[recipient_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])
    mommy.make(LegalEntity, business_categories=expected_business_cat, recipient_name=recipient_name,
               recipient_unique_id=recipient_duns)

    business_cat = recipients.extract_business_categories(recipient_name, recipient_duns)
    assert business_cat == expected_business_cat


@pytest.mark.django_db
def test_obtain_recipient_totals_year(mock_matviews_qs):
    """ Testing recipient totals with different year values """
    # Testing with specific child
    recipient_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    recipient_hash = recipient_id[:-2]
    # load all of the transactions
    mock_transactions = []
    for category, transaction in TEST_UNIVERSAL_TRANSACTIONS.items():
        transaction['recipient_hash'] = recipient_hash
        transaction['parent_recipient_unique_id'] = '000000009'
        mock_transactions.append(MockModel(**transaction))
    add_to_mock_objects(mock_matviews_qs, mock_transactions)

    # Latest
    expected_total = 100
    expected_count = 1
    total, count = recipients.obtain_recipient_totals(recipient_id, year='latest', subawards=False)
    assert total == expected_total
    assert count == expected_count

    # All
    expected_total = 350
    expected_count = 3
    total, count = recipients.obtain_recipient_totals(recipient_id, year='all', subawards=False)
    assert total == expected_total
    assert count == expected_count

    # FY2016
    expected_total = 50
    expected_count = 1
    total, count = recipients.obtain_recipient_totals(recipient_id, year='2016', subawards=False)
    assert total == expected_total
    assert count == expected_count


@pytest.mark.django_db
def test_obtain_recipient_totals_parent(mock_matviews_qs, ):
    """ Testing recipient totals with parent child relationships """
    # Testing with specific parent/child ids
    parent_id = '00077a9a-5a70-8919-fd19-330762af6b84-P'
    child1_id = '00077a9a-5a70-8919-fd19-330762af6b84-C'
    child1_hash = child1_id[:-2]
    parent_child1_duns = '000000001'
    child2_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    child2_hash = child2_id[:-2]
    other_id = '00002940-fdbe-3fc5-9252-d46c0ae8758c-R'
    transaction_hash_map = {
        '1': {
            'hash': child1_hash,
            'parent_duns': parent_child1_duns
        },
        '2': {
            'hash': child2_hash,
            'parent_duns': parent_child1_duns,
        },
        '3': {
            'hash': other_id,
            'parent_duns': None
        }
    }

    # load recipient profiles
    for recipient_id, recipient_profile in TEST_RECIPIENT_PROFILES.items():
        mommy.make(RecipientProfile, **recipient_profile)

    # load transactions for each child and parent (making sure it's excluded)
    mock_transactions = []
    for category, transaction in TEST_UNIVERSAL_TRANSACTIONS.items():
        transaction['recipient_hash'] = transaction_hash_map[transaction['transaction_id']]['hash']
        transaction['parent_recipient_unique_id'] = transaction_hash_map[transaction['transaction_id']]['parent_duns']
        mock_transactions.append(MockModel(**transaction))
    add_to_mock_objects(mock_matviews_qs, mock_transactions)

    expected_total = 150
    expected_count = 2
    total, count = recipients.obtain_recipient_totals(parent_id, year='all', subawards=False)
    assert total == expected_total
    assert count == expected_count


def recipient_overview_endpoint(id, year='latest'):
    endpoint = '/api/v2/recipient/duns/{}/'.format(id)
    if year:
        endpoint = '{}?year={}'.format(endpoint, year)
    return endpoint


@pytest.mark.django_db
def test_recipient_overview(client, mock_matviews_qs, mock_reference_matviews):
    """ Testing a simple example of the endpoint as a whole """
    r_id = '00077a9a-5a70-8919-fd19-330762af6b84-C'
    recipient_hash = r_id[:-2]

    # Mock Transactions
    mock_transactions = []
    for category, transaction in TEST_UNIVERSAL_TRANSACTIONS.items():
        transaction['recipient_hash'] = recipient_hash
        mock_transactions.append(MockModel(**transaction))
    add_to_mock_objects(mock_matviews_qs, mock_transactions)

    # Mock Recipient Profiles
    for recipient_id, recipient_profile in TEST_RECIPIENT_PROFILES.items():
        mommy.make(RecipientProfile, **recipient_profile)

    # Mock Recipient Lookups
    mock_lookups = []
    for recipient_hash, recipient_lookup in TEST_RECIPIENT_LOOKUPS.items():
        mock_lookups.append(MockModel(**recipient_lookup))
    add_to_mock_objects(mock_reference_matviews, mock_lookups)

    # Mock DUNS
    for duns, duns_dict in TEST_DUNS.items():
        test_duns_model = duns_dict.copy()
        test_duns_model.pop('business_types_codes')
        country_code = test_duns_model['country_code']
        mommy.make(DUNS, **test_duns_model)
        mommy.make(RefCountryCode, **TEST_REF_COUNTRY_CODE[country_code])

    # Mock Legal Entity
    expected_business_cat = ['expected', 'business', 'cat']
    mommy.make(LegalEntity, business_categories=expected_business_cat, recipient_name='PARENT RECIPIENT',
               recipient_unique_id='000000001')

    resp = client.get(recipient_overview_endpoint(r_id))
    assert resp.status_code == status.HTTP_200_OK
    expected = {
        'name': 'PARENT RECIPIENT',
        'duns': '000000001',
        'recipient_id': '00077a9a-5a70-8919-fd19-330762af6b84-C',
        'recipient_level': 'C',
        'parent_name': 'PARENT RECIPIENT',
        'parent_duns': '000000001',
        'parent_id': '00077a9a-5a70-8919-fd19-330762af6b84-P',
        'business_types': ['expected', 'business', 'cat'],
        'location': {
            'address_line1': 'PARENT ADDRESS LINE 1',
            'address_line2': 'PARENT ADDRESS LINE 2',
            'address_line3': None,
            'county_name': None,
            'city_name': 'PARENT CITY',
            'congressional_code': 'PARENT CONGRESSIONAL DISTRICT',
            'country_code': 'PARENT COUNTRY CODE',
            'country_name': 'PARENT COUNTRY NAME',
            'state_code': 'PARENT STATE',
            'zip': 'PARENT ZIP',
            'zip4': 'PARENT ZIP4',
            'foreign_province': None,
            'foreign_postal_code': None
        },
        'total_transaction_amount': 100,
        'total_transactions': 1
    }
    assert resp.data == expected


@pytest.mark.django_db
def test_extract_hash_name_from_duns(mock_reference_matviews):
    """ Testing extracting the hash/name from a DUNS """
    example_duns = '000000001'
    expected_hash = '00077a9a-5a70-8919-fd19-330762af6b84'
    expected_name = 'PARENT RECIPIENT'
    mock_recipient_lookup = MockModel(**TEST_RECIPIENT_LOOKUPS[expected_hash])
    add_to_mock_objects(mock_reference_matviews, [mock_recipient_lookup])

    recipient_hash, recipient_name = recipients.extract_hash_name_from_duns(example_duns)
    assert expected_hash == recipient_hash
    assert expected_name == recipient_name


def recipient_children_endpoint(duns, year='latest'):
    endpoint = '/api/v2/recipient/children/{}/'.format(duns)
    if year:
        endpoint = '{}?year={}'.format(endpoint, year)
    return endpoint


@pytest.mark.django_db
def test_child_recipient_success(client, mock_reference_matviews, mock_matviews_qs):
    """ Testing successfull child recipient calls """
    child1_id = '00077a9a-5a70-8919-fd19-330762af6b84-C'
    child1_hash = child1_id[:-2]
    parent_child1_duns = '000000001'
    child2_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    child2_hash = child2_id[:-2]
    child2_duns = '000000002'
    other_id = '00002940-fdbe-3fc5-9252-d46c0ae8758c-R'
    transaction_hash_map = {
        '1': {
            'hash': child1_hash,
            'duns': parent_child1_duns,
            'parent_duns': parent_child1_duns
        },
        '2': {
            'hash': child2_hash,
            'duns': child2_duns,
            'parent_duns': parent_child1_duns
        },
        '3': {
            'hash': other_id,
            'duns': None,
            'parent_duns': None
        }
    }

    # Mock Recipient Profiles
    for recipient_id, recipient_profile in TEST_RECIPIENT_PROFILES.items():
        mommy.make(RecipientProfile, **recipient_profile)

    # Mock Recipient Lookups
    mock_lookups = []
    for recipient_hash, recipient_lookup in TEST_RECIPIENT_LOOKUPS.items():
        mock_lookups.append(MockModel(**recipient_lookup))
    add_to_mock_objects(mock_reference_matviews, mock_lookups)

    # Mock DUNS
    for duns, duns_dict in TEST_DUNS.items():
        test_duns_model = duns_dict.copy()
        country_code = test_duns_model['country_code']
        mommy.make(DUNS, **test_duns_model)
        mommy.make(RefCountryCode, **TEST_REF_COUNTRY_CODE[country_code])

    # load transactions for each child and parent (making sure it's excluded)
    mock_transactions = []
    for category, transaction in TEST_UNIVERSAL_TRANSACTIONS.items():
        transaction['recipient_hash'] = transaction_hash_map[transaction['transaction_id']]['hash']
        transaction['recipient_unique_id'] = transaction_hash_map[transaction['transaction_id']]['duns']
        transaction['parent_recipient_unique_id'] = transaction_hash_map[transaction['transaction_id']]['parent_duns']
        mock_transactions.append(MockModel(**transaction))
    add_to_mock_objects(mock_matviews_qs, mock_transactions)

    # Ignoring nonexistent child duns - 000000005
    child1_object = {
        'recipient_id': child1_id,
        'name': 'PARENT RECIPIENT',
        'duns': parent_child1_duns,
        'amount': 100,
        'state_province': 'PARENT STATE'
    }
    child2_object = {
        'recipient_id': child2_id,
        'name': 'CHILD RECIPIENT',
        'duns': child2_duns,
        'amount': 50,
        'state_province': 'CHILD STATE'
    }
    expected = [child1_object, child2_object]
    resp = client.get(recipient_children_endpoint(parent_child1_duns, 'all'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == sorted(expected, key=lambda key: key['recipient_id'])


@pytest.mark.django_db
def test_child_recipient_failures(client, mock_reference_matviews, mock_matviews_qs):
    """ Testing failed child recipient calls """

    child1_id = '00077a9a-5a70-8919-fd19-330762af6b84-C'
    child1_hash = child1_id[:-2]
    parent_child1_duns = '000000001'
    child2_id = '392052ae-92ab-f3f4-d9fa-b57f45b7750b-C'
    child2_hash = child2_id[:-2]
    child2_duns = '000000002'
    other_id = '00002940-fdbe-3fc5-9252-d46c0ae8758c-R'
    transaction_hash_map = {
        '1': {
            'hash': child1_hash,
            'duns': parent_child1_duns
        },
        '2': {
            'hash': child2_hash,
            'duns': child2_duns,
        },
        '3': {
            'hash': other_id,
            'duns': None
        }
    }

    # Mock Recipient Profiles
    for recipient_id, recipient_profile in TEST_RECIPIENT_PROFILES.items():
        mommy.make(RecipientProfile, **recipient_profile)

    # Mock Recipient Lookups
    mock_lookups = []
    for recipient_hash, recipient_lookup in TEST_RECIPIENT_LOOKUPS.items():
        mock_lookups.append(MockModel(**recipient_lookup))
    add_to_mock_objects(mock_reference_matviews, mock_lookups)

    # load transactions for each child and parent (making sure it's excluded)
    mock_transactions = []
    for category, transaction in TEST_UNIVERSAL_TRANSACTIONS.items():
        transaction['recipient_hash'] = transaction_hash_map[transaction['transaction_id']]['hash']
        transaction['recipient_unique_id'] = transaction_hash_map[transaction['transaction_id']]['duns']
        mock_transactions.append(MockModel(**transaction))
    add_to_mock_objects(mock_matviews_qs, mock_transactions)

    # Testing for child DUNS
    resp = client.get(recipient_children_endpoint(child2_duns, 'all'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data['detail'] == 'DUNS is not listed as a parent: \'{}\'.'.format(child2_duns)

    # Testing for non-existent DUNS
    non_existent_duns = '000000003'
    resp = client.get(recipient_children_endpoint(non_existent_duns, 'all'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data['detail'] == 'DUNS not found: \'{}\'.'.format(non_existent_duns)
