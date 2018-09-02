# Stdlib imports

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest
from unittest.mock import MagicMock
from model_mommy import mommy

# Imports from your apps
from usaspending_api.awards.models import Award, Subaward, TransactionNormalized
from usaspending_api.references.models import Agency, SubtierAgency
from usaspending_api.recipient.models import RecipientLookup


DB_CURSOR_PARAMS = {
    'default': MagicMock(),
    'data_broker': MagicMock(),
    'data_broker_data_file': 'usaspending_api/broker/tests/data/broker_subawards.json'
}


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_fresh_subaward_load_no_associated_awards(mock_db_cursor):
    """
    Test the subaward load as if it were happening for the first time on an empty table, with no awards to link to
    """
    mommy.make(RecipientLookup, duns='PARENTDUNS54321', legal_business_name='WIZARD SCHOOLS')
    call_command('load_fsrs')

    expected_results = {
        'count': 3,
        'awards': [None, None, None],
        'recipient_names': ["JJ'S DINER", 'HARRY POTTER', 'HARRY POTTER'],
        'ppop_city_names': ['PAWNEE', '', ''],
        'subaward_descs': ['RANDOM DESCRIPTION TEXT', 'HOGWARTS ACCEPTANCE LETTER',
                           'HOGWARTS ACCEPTANCE LETTER REVISED'],
        'duns': ['DUNS12345', 'DUNS54321', 'DUNS54321'],
        'dba_names': ["JJ'S", "HOGWARTS", "HOGWARTS"],
        'parent_recipient_names': ["PARENT JJ'S DINER", "WIZARD SCHOOLS", "WIZARD SCHOOLS"],
        'broker_award_ids': [10, 20, 30],
        'internal_ids': ['PROCUREMENT_INTERNAL_ID', 'GRANT_INTERNAL_ID_1', 'GRANT_INTERNAL_ID_2'],
    }

    actual_results = {
        'count': Subaward.objects.count(),
        'awards': list(Subaward.objects.values_list('award', flat=True)),
        'recipient_names': list(Subaward.objects.values_list('recipient_name', flat=True)),
        'ppop_city_names': list(Subaward.objects.values_list('pop_city_name', flat=True)),
        'subaward_descs': list(Subaward.objects.values_list('description', flat=True)),
        'duns': list(Subaward.objects.values_list('recipient_unique_id', flat=True)),
        'dba_names': list(Subaward.objects.values_list('dba_name', flat=True)),
        'parent_recipient_names': list(Subaward.objects.values_list('parent_recipient_name', flat=True)),
        'broker_award_ids': list(Subaward.objects.values_list('broker_award_id', flat=True)),
        'internal_ids': list(Subaward.objects.values_list('internal_id', flat=True)),
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_fresh_subaward_load_associated_awards_exact_match(mock_db_cursor):
    """
    Test the subaward load as if it were happening for the first time on an empty table, with no awards to link to
    """

    # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
    # "CONT_AW_" + contract_agency_code + contract_idv_agency_code + contract_number + idv_reference_number
    models_to_mock = [
        {
            'model': Award,
            'id': 50,
            'generated_unique_award_id': 'CONT_AW_12345_12345_PIID12345_IDV12345',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': Award,
            'id': 100,
            'fain': 'FAIN54321',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': SubtierAgency,
            'subtier_agency_id': 1,
            'subtier_code': '12345'
        },
        {
            'model': Agency,
            'subtier_agency_id': 1
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_fsrs')

    expected_results = {
        'count': 3,
        'award_ids': [50, 100, 100]

    }

    actual_results = {
        'count': Subaward.objects.count(),
        'award_ids': list(Subaward.objects.values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_fresh_subaward_load_associated_awards_with_dashes(mock_db_cursor):
    """
    Test the subaward load as if it were happening for the first time on an empty table, with no awards to link to
    """

    # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
    # "CONT_AW_" + contract_agency_code + contract_idv_agency_code + contract_number + idv_reference_number
    models_to_mock = [
        {
            'model': Award,
            'id': 50,
            'generated_unique_award_id': 'CONT_AW_12345_12345_PIID12345_IDV12345',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': Award,
            'id': 100,
            'fain': 'FAIN-54321',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': SubtierAgency,
            'subtier_agency_id': 1,
            'subtier_code': '12345'
        },
        {
            'model': Agency,
            'subtier_agency_id': 1
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_fsrs')

    expected_results = {
        'count': 3,
        'award_ids': [50, 100, 100]

    }

    actual_results = {
        'count': Subaward.objects.count(),
        'award_ids': list(Subaward.objects.values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_fresh_subaward_load_associated_awards_multiple_matching_fains(mock_db_cursor):
    """
    Test the subaward load as if it were happening for the first time on an empty table, with no awards to link to
    """

    # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
    # "CONT_AW_" + contract_agency_code + contract_idv_agency_code + contract_number + idv_reference_number
    models_to_mock = [
        {
            'model': Award,
            'id': 50,
            'generated_unique_award_id': 'CONT_AW_12345_12345_PIID12345_IDV12345',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': Award,
            'id': 99,
            'fain': 'FAIN54321',
            'date_signed': '1700-01-02',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': Award,
            'id': 100,
            'fain': 'FAIN-54321',
            'date_signed': '1700-01-01',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': SubtierAgency,
            'subtier_agency_id': 1,
            'subtier_code': '12345'
        },
        {
            'model': Agency,
            'subtier_agency_id': 1
        }
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_fsrs')

    expected_results = {
        'count': 3,
        'award_ids': [50, 99, 99]

    }

    actual_results = {
        'count': Subaward.objects.count(),
        'award_ids': list(Subaward.objects.values_list('award_id', flat=True))
    }

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_subaward_update(mock_db_cursor):
    """
    Test the subaward load as if a subaward is already in there with the same internal id, delete/update it
    """

    models_to_mock = [
        {
            'model': Award,
            'id': 99,
            'fain': 'FAIN54321',
            'date_signed': '1700-01-03',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': SubtierAgency,
            'subtier_agency_id': 1,
            'subtier_code': '12345'
        },
        {
            'model': Agency,
            'subtier_agency_id': 1
        },
        {
            'id': 5,
            'model': Subaward,
            'subaward_number': 'SUBNUM54322',
            'internal_id': 'GRANT_INTERNAL_ID_1',
            'broker_award_id': 1,
            'award_type': 'grant',
            'award_id': 99,
            'amount': 2,
            'action_date': '2014-01-01'
        },
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_fsrs')

    expected_results = {
        'subaward_number': 'SUBNUM54321',
        'amount': 54321,
        'action_date': '1212-12-12'
    }

    actual_results = Subaward.objects.filter(internal_id='GRANT_INTERNAL_ID_1').values(*expected_results)[0]
    actual_results['action_date'] = str(actual_results['action_date'])

    assert expected_results == actual_results


@pytest.mark.django_db
@pytest.mark.parametrize('mock_db_cursor', [DB_CURSOR_PARAMS.copy()], indirect=True)
def test_subaward_broken_links(mock_db_cursor):
    """
    Test the subaward load as if a subaward has been loaded w/o a parent award and now the parent award is available
    """

    models_to_mock = [
        {
            'model': Award,
            'id': 99,
            'fain': 'FAIN54321',
            'date_signed': '1700-01-03',
            'latest_transaction': mommy.make(TransactionNormalized)
        },
        {
            'model': SubtierAgency,
            'subtier_agency_id': 1,
            'subtier_code': '12345'
        },
        {
            'model': Agency,
            'subtier_agency_id': 1
        },
        {
            'id': 5,
            'model': Subaward,
            'subaward_number': 'SUBNUM54322',
            'internal_id': 'GRANT_INTERNAL_ID_1',
            'broker_award_id': 100,
            'award_type': 'grant',
            'award_id': None,
            'amount': 2,
            'action_date': '2014-01-01'
        },
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop('model'), **entry)

    call_command('load_fsrs')

    expected_results = {
        'award_id': 99,
    }

    actual_results = Subaward.objects.filter(id=5).values(*expected_results)[0]

    assert expected_results == actual_results
