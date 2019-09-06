import pytest

from decimal import Decimal
from django.db import connection
from django_mock_queries.query import MockModel, MockSet
from model_mommy import mommy
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.spending_by_category import BusinessLogic


def test_category_awarding_agency_awards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name="Department of Pizza", abbreviation="DOP")
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(
        awarding_agency_id=2,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=5,
    )
    mock_model_2 = MockModel(
        awarding_agency_id=3,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])
    add_to_mock_objects(mock_agencies["toptier_agency"], [mock_toptier])

    test_payload = {"category": "awarding_agency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 15, "name": "Department of Pizza", "code": "DOP", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_agency_subawards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name="Department of Pizza", abbreviation="DOP")
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(
        awarding_agency_id=2,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        amount=5,
    )
    mock_model_2 = MockModel(
        awarding_agency_id=3,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        amount=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "awarding_agency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 15, "name": "Department of Pizza", "code": "DOP", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_awards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name="Department of Sub-pizza", abbreviation="DOSP")
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(
        awarding_agency_id=2,
        awarding_subtier_agency_name="Department of Sub-pizza",
        awarding_subtier_agency_abbreviation="DOSP",
        generated_pragmatic_obligation=10,
    )
    mock_model_2 = MockModel(
        awarding_agency_id=3,
        awarding_subtier_agency_name="Department of Sub-pizza",
        awarding_subtier_agency_abbreviation="DOSP",
        generated_pragmatic_obligation=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "awarding_subagency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 20, "name": "Department of Sub-pizza", "code": "DOSP", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_subawards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name="Department of Sub-pizza", abbreviation="DOSP")
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(
        awarding_agency_id=2,
        awarding_subtier_agency_name="Department of Sub-pizza",
        awarding_subtier_agency_abbreviation="DOSP",
        amount=10,
    )
    mock_model_2 = MockModel(
        awarding_agency_id=3,
        awarding_subtier_agency_name="Department of Sub-pizza",
        awarding_subtier_agency_abbreviation="DOSP",
        amount=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "awarding_subagency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 20, "name": "Department of Sub-pizza", "code": "DOSP", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_awards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name="Department of Calzone", abbreviation="DOC")
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(
        funding_agency_id=2,
        funding_toptier_agency_name="Department of Calzone",
        funding_toptier_agency_abbreviation="DOC",
        generated_pragmatic_obligation=50,
    )
    mock_model_2 = MockModel(
        funding_agency_id=3,
        funding_toptier_agency_name="Department of Calzone",
        funding_toptier_agency_abbreviation="DOC",
        generated_pragmatic_obligation=50,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "funding_agency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 100, "name": "Department of Calzone", "code": "DOC", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_subawards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name="Department of Calzone", abbreviation="DOC")
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(
        funding_agency_id=2,
        funding_toptier_agency_name="Department of Calzone",
        funding_toptier_agency_abbreviation="DOC",
        amount=50,
    )
    mock_model_2 = MockModel(
        funding_agency_id=3,
        funding_toptier_agency_name="Department of Calzone",
        funding_toptier_agency_abbreviation="DOC",
        amount=50,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "funding_agency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 100, "name": "Department of Calzone", "code": "DOC", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_awards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name="Department of Sub-calzone", abbreviation="DOSC")
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(
        funding_agency_id=2,
        funding_subtier_agency_name="Department of Sub-calzone",
        funding_subtier_agency_abbreviation="DOSC",
        generated_pragmatic_obligation=5,
    )
    mock_model_2 = MockModel(
        funding_agency_id=3,
        funding_subtier_agency_name="Department of Sub-calzone",
        funding_subtier_agency_abbreviation="DOSC",
        generated_pragmatic_obligation=-5,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "funding_subagency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 0, "name": "Department of Sub-calzone", "code": "DOSC", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_subawards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name="Department of Sub-calzone", abbreviation="DOSC")
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(
        funding_agency_id=2,
        funding_subtier_agency_name="Department of Sub-calzone",
        funding_subtier_agency_abbreviation="DOSC",
        amount=5,
    )
    mock_model_2 = MockModel(
        funding_agency_id=3,
        funding_subtier_agency_name="Department of Sub-calzone",
        funding_subtier_agency_abbreviation="DOSC",
        amount=-5,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies["agency"], [mock_agency, mock_agency_1])

    test_payload = {"category": "funding_subagency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 0, "name": "Department of Sub-calzone", "code": "DOSC", "id": 2}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_recipient_duns_awards(mock_matviews_qs):
    mock_model_1 = MockModel(recipient_hash="59f9a646-cd1c-cbdc-63dd-1020fac59336", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(recipient_hash="59f9a646-cd1c-cbdc-63dd-1020fac59336", generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(recipient_hash="3725ba78-a607-7ab4-1cf6-2a08207bac3c", generated_pragmatic_obligation=1)
    mock_model_4 = MockModel(recipient_hash="3725ba78-a607-7ab4-1cf6-2a08207bac3c", generated_pragmatic_obligation=10)
    mock_model_5 = MockModel(recipient_hash="18569a71-3b0a-1586-50a9-cbb8bb070136", generated_pragmatic_obligation=15)

    mommy.make(
        "recipient.RecipientLookup",
        recipient_hash="3725ba78-a607-7ab4-1cf6-2a08207bac3c",
        legal_business_name="John Doe",
        duns="1234JD4321",
    )
    mommy.make(
        "recipient.RecipientLookup",
        recipient_hash="59f9a646-cd1c-cbdc-63dd-1020fac59336",
        legal_business_name="University of Pawnee",
        duns="00UOP00",
    )
    mommy.make(
        "recipient.RecipientLookup",
        recipient_hash="18569a71-3b0a-1586-50a9-cbb8bb070136",
        legal_business_name="MULTIPLE RECIPIENTS",
        duns=None,
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4, mock_model_5])

    test_payload = {"category": "recipient_duns", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "recipient_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 15, "name": "MULTIPLE RECIPIENTS", "code": None, "recipient_id": None},
            {"amount": 11, "name": "John Doe", "code": "1234JD4321", "recipient_id": None},
            {"amount": 2, "name": "University of Pawnee", "code": "00UOP00", "recipient_id": None},
        ],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_recipient_duns_subawards():

    mommy.make("awards.Subaward", id=1, recipient_name="University of Pawnee", recipient_unique_id="00UOP00", amount=1)
    mommy.make("awards.Subaward", id=2, recipient_name="University of Pawnee", recipient_unique_id="00UOP00", amount=1)
    mommy.make("awards.Subaward", id=3, recipient_name="John Doe", recipient_unique_id="1234JD4321", amount=1)
    mommy.make("awards.Subaward", id=4, recipient_name="John Doe", recipient_unique_id="1234JD4321", amount=10)
    mommy.make("awards.Subaward", id=5, recipient_name="MULTIPLE RECIPIENTS", recipient_unique_id=None, amount=15)

    mommy.make("recipient.RecipientLookup", duns="00UOP00", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a948")
    mommy.make("recipient.RecipientLookup", duns="1234JD4321", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a949")
    mommy.make("recipient.RecipientLookup", duns=None, recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a940")

    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id="00UOP00",
        recipient_level="P",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a948",
        recipient_name="University of Pawnee",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id="1234JD4321",
        recipient_level="C",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a949",
        recipient_name="John Doe",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id=None,
        recipient_level="R",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a940",
        recipient_name="MULTIPLE RECIPIENTS",
    )

    with connection.cursor() as cursor:
        cursor.execute("refresh materialized view concurrently subaward_view")

    test_payload = {"category": "recipient_duns", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "recipient_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": Decimal(15),
                "name": "MULTIPLE RECIPIENTS",
                "code": None,
                "recipient_id": None,
            },
            {
                "amount": Decimal(11),
                "name": "JOHN DOE",
                "code": "1234JD4321",
                "recipient_id": "f9006d7e-fa6c-fa1c-6bc5-964fe524a949-C",
            },
            {
                "amount": Decimal(2),
                "name": "UNIVERSITY OF PAWNEE",
                "code": "00UOP00",
                "recipient_id": "f9006d7e-fa6c-fa1c-6bc5-964fe524a948-P",
            },
        ],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting recipient parent duns")
def test_category_recipient_parent_duns_awards(mock_matviews_qs, mock_recipients):
    mock_recipient_1 = MockModel(recipient_unique_id="00UOP00", legal_entity_id=1)
    mock_recipient_2 = MockModel(recipient_unique_id="1234JD4321", legal_entity_id=2)
    mock_model_1 = MockModel(
        recipient_name="University of Pawnee", parent_recipient_unique_id="00UOP00", generated_pragmatic_obligation=1
    )
    mock_model_2 = MockModel(
        recipient_name="University of Pawnee", parent_recipient_unique_id="00UOP00", generated_pragmatic_obligation=1
    )
    mock_model_3 = MockModel(
        recipient_name="John Doe", parent_recipient_unique_id="1234JD4321", generated_pragmatic_obligation=1
    )
    mock_model_4 = MockModel(
        recipient_name="John Doe", parent_recipient_unique_id="1234JD4321", generated_pragmatic_obligation=10
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_recipients, [mock_recipient_1, mock_recipient_2])

    test_payload = {"category": "recipient_parent_duns", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "recipient_parent_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 11, "name": "John Doe", "code": "1234JD4321", "id": 2},
            {"amount": 2, "name": "University of Pawnee", "code": "00UOP00", "id": 1},
        ],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting recipient parent duns")
def test_category_recipient_parent_duns_subawards(mock_matviews_qs, mock_recipients):
    mock_recipient_1 = MockModel(recipient_unique_id="00UOP00", legal_entity_id=1)
    mock_recipient_2 = MockModel(recipient_unique_id="1234JD4321", legal_entity_id=2)
    mock_model_1 = MockModel(recipient_name="University of Pawnee", parent_recipient_unique_id="00UOP00", amount=1)
    mock_model_2 = MockModel(recipient_name="University of Pawnee", parent_recipient_unique_id="00UOP00", amount=1)
    mock_model_3 = MockModel(recipient_name="John Doe", parent_recipient_unique_id="1234JD4321", amount=1)
    mock_model_4 = MockModel(recipient_name="John Doe", parent_recipient_unique_id="1234JD4321", amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_recipients, [mock_recipient_1, mock_recipient_2])

    test_payload = {"category": "recipient_parent_duns", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "recipient_parent_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 11, "name": "John Doe", "code": "1234JD4321", "id": 2},
            {"amount": 2, "name": "University of Pawnee", "code": "00UOP00", "id": 1},
        ],
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_awards(mock_matviews_qs, mock_cfda):
    mock_model_cfda = MockModel(program_title="CFDA TITLE 1234", program_number="CFDA1234", id=1)
    mock_model_1 = MockModel(cfda_number="CFDA1234", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(cfda_number="CFDA1234", generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_cfda, [mock_model_cfda])

    test_payload = {"category": "cfda", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1}],
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_subawards(mock_matviews_qs, mock_cfda):
    mock_model_cfda = MockModel(program_title="CFDA TITLE 1234", program_number="CFDA1234", id=1)
    mock_model_1 = MockModel(cfda_number="CFDA1234", amount=1)
    mock_model_2 = MockModel(cfda_number="CFDA1234", amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_cfda, [mock_model_cfda])

    test_payload = {"category": "cfda", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1}],
    }

    assert expected_response == spending_by_category_logic


def test_category_psc_awards(mock_matviews_qs, mock_psc):
    mock_psc_1 = MockModel(code="PSC 1234", description="PSC DESCRIPTION UP")
    mock_psc_2 = MockModel(code="PSC 9876", description="PSC DESCRIPTION DOWN")
    mock_model_1 = MockModel(product_or_service_code="PSC 1234", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(product_or_service_code="PSC 1234", generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(product_or_service_code="PSC 9876", generated_pragmatic_obligation=2)
    mock_model_4 = MockModel(product_or_service_code="PSC 9876", generated_pragmatic_obligation=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_psc, [mock_psc_1, mock_psc_2])

    test_payload = {"category": "psc", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "psc",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "PSC 9876", "id": None, "name": "PSC DESCRIPTION DOWN"},
            {"amount": 2, "code": "PSC 1234", "id": None, "name": "PSC DESCRIPTION UP"},
        ],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting psc subawards")
def test_category_psc_subawards(mock_matviews_qs, mock_psc):
    mock_psc_1 = MockModel(code="PSC 1234", description="PSC DESCRIPTION UP")
    mock_psc_2 = MockModel(code="PSC 9876", description="PSC DESCRIPTION DOWN")
    mock_model_1 = MockModel(product_or_service_code="PSC 1234", amount=1)
    mock_model_2 = MockModel(product_or_service_code="PSC 1234", amount=1)
    mock_model_3 = MockModel(product_or_service_code="PSC 9876", amount=2)
    mock_model_4 = MockModel(product_or_service_code="PSC 9876", amount=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_psc, [mock_psc_1, mock_psc_2])

    test_payload = {"category": "psc", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "psc",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "PSC 9876", "id": None, "name": "PSC DESCRIPTION DOWN"},
            {"amount": 2, "code": "PSC 1234", "id": None, "name": "PSC DESCRIPTION UP"},
        ],
    }

    assert expected_response == spending_by_category_logic


def test_category_naics_awards(mock_matviews_qs, mock_naics):
    mock_model_1 = MockModel(
        naics_code="NAICS 1234", naics_description="NAICS DESC 1234", generated_pragmatic_obligation=1
    )
    mock_model_2 = MockModel(
        naics_code="NAICS 1234", naics_description="NAICS DESC 1234", generated_pragmatic_obligation=1
    )
    mock_model_3 = MockModel(
        naics_code="NAICS 9876", naics_description="NAICS DESC 9876", generated_pragmatic_obligation=2
    )
    mock_model_4 = MockModel(
        naics_code="NAICS 9876", naics_description="NAICS DESC 9876", generated_pragmatic_obligation=2
    )

    mock_naics_1 = MockModel(code="NAICS 1234", description="SOURCE NAICS DESC 1234", year=1955)
    mock_naics_2 = MockModel(code="NAICS 9876", description="SOURCE NAICS DESC 9876", year=1985)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_naics, [mock_naics_1, mock_naics_2])

    test_payload = {"category": "naics", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "naics",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "NAICS 9876", "name": "SOURCE NAICS DESC 9876", "id": None},
            {"amount": 2, "code": "NAICS 1234", "name": "SOURCE NAICS DESC 1234", "id": None},
        ],
    }

    assert expected_response == spending_by_category_logic


def test_category_county_awards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_county_code="04", pop_county_name="COUNTYSVILLE", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_county_code="04", pop_county_name="COUNTYSVILLE", generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "county", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "04", "name": "COUNTYSVILLE", "id": None}],
    }

    assert expected_response == spending_by_category_logic


def test_category_county_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_county_code="04", pop_county_name="COUNTYSVILLE", amount=1)
    mock_model_2 = MockModel(pop_county_code="04", pop_county_name="COUNTYSVILLE", amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "county", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "04", "name": "COUNTYSVILLE", "id": None}],
    }

    assert expected_response == spending_by_category_logic


def test_category_district_awards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code="06", pop_state_code="XY", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_congressional_code="06", pop_state_code="XY", generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "district", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "06", "name": "XY-06", "id": None}],
    }

    assert expected_response == spending_by_category_logic


def test_category_district_awards_multiple_districts(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code="90", pop_state_code="XY", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_congressional_code="90", pop_state_code="XY", generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "district", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "90", "name": "XY-MULTIPLE DISTRICTS", "id": None}],
    }

    assert expected_response == spending_by_category_logic


def test_category_district_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code="06", pop_state_code="XY", amount=1)
    mock_model_2 = MockModel(pop_congressional_code="06", pop_state_code="XY", amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "district", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "06", "name": "XY-06", "id": None}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory(mock_matviews_qs):
    mock_model_1 = MockModel(pop_state_code="XY", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_state_code="XY", generated_pragmatic_obligation=1)
    mommy.make("recipient.StateData", name="Test State", code="XY")

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "state_territory", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "XY", "name": "Test State", "id": None}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_state_code="XY", amount=1)
    mock_model_2 = MockModel(pop_state_code="XY", amount=1)
    mommy.make("recipient.StateData", name="Test State", code="XY")

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "state_territory", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "XY", "name": "Test State", "id": None}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country(mock_matviews_qs):
    mock_model_1 = MockModel(pop_country_code="US", generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_country_code="US", generated_pragmatic_obligation=1)
    mommy.make("references.RefCountryCode", country_name="UNITED STATES", country_code="US")
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "country", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "US", "name": "UNITED STATES", "id": None}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_country_code="US", amount=1)
    mock_model_2 = MockModel(pop_country_code="US", amount=1)
    mommy.make("references.RefCountryCode", country_name="UNITED STATES", country_code="US")
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {"category": "country", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "US", "name": "UNITED STATES", "id": None}],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_federal_accounts(mock_matviews_qs):

    mock_model_1 = MockModel(
        federal_account_id=10,
        federal_account_display="020-0001",
        account_title="Test Federal Account",
        recipient_hash="00000-00000-00000-00000-00000",
        parent_recipient_unique_id="000000",
        generated_pragmatic_obligation=1,
    )
    mock_model_2 = MockModel(
        federal_account_id=10,
        federal_account_display="020-0001",
        account_title="Test Federal Account",
        recipient_hash="00000-00000-00000-00000-00000",
        parent_recipient_unique_id="000000",
        generated_pragmatic_obligation=2,
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        "category": "federal_account",
        "filters": {"recipient_id": "00000-00000-00000-00000-00000-C"},
        "subawards": False,
        "page": 1,
        "limit": 50,
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "federal_account",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 3, "code": "020-0001", "name": "Test Federal Account", "id": 10}],
    }

    assert expected_response == spending_by_category_logic


# Keeping this skipped until Recipient Hash gets included in the Subaward View
@pytest.mark.skip
def test_category_federal_accounts_subawards(
    mock_matviews_qs, mock_federal_account, mock_tas, mock_award, mock_financial_account, mock_transaction
):
    fa = MockModel(id=10, agency_identifier="020", main_account_code="0001", account_title="Test Federal Account")
    add_to_mock_objects(mock_federal_account, [fa])

    tas = MockModel(treasury_account_identifier=2, federal_account_id=10)
    add_to_mock_objects(mock_tas, [tas])

    award = MockModel(id=3)
    add_to_mock_objects(mock_award, [award])

    fs = MockModel(financial_accounts_by_awards_id=4, submission_id=3, treasury_account=tas, award=award)
    add_to_mock_objects(mock_financial_account, [fs])

    award.financial_set = MockSet(fs)
    t1 = MockModel(award=award, id=5)
    t2 = MockModel(award=award, id=6)
    add_to_mock_objects(mock_transaction, [t1, t2])

    mock_model_1 = MockModel(
        transaction=t1, recipient_hash="00000-00000-00000-00000-00000", parent_recipient_unique_id="000000", amount=1
    )
    mock_model_2 = MockModel(
        transaction=t2, recipient_hash="00000-00000-00000-00000-00000", parent_recipient_unique_id="000000", amount=1
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        "category": "federal_account",
        "filters": {"recipient_id": "00000-00000-00000-00000-00000-C"},
        "subawards": True,
        "page": 1,
        "limit": 50,
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        "category": "federal_account",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "020-0001", "name": "Test Federal Account", "id": 10}],
    }

    assert expected_response == spending_by_category_logic
