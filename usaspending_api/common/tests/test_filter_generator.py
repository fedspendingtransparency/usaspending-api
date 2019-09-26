import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.common.api_request_utils import FilterGenerator


@pytest.fixture
def mock_data():
    """mock data"""
    toptier = mommy.make("references.ToptierAgency", toptier_agency_id=11, name="LEXCORP")
    agency = mommy.make("references.Agency", id=10, toptier_agency=toptier)
    mommy.make(
        "awards.Award",
        id=1234,
        piid="zzz",
        fain="abc123",
        type="B",
        awarding_agency=agency,
        total_obligation=1000,
        description="SMALL BUSINESS ADMINISTRATION",
        date_signed=date(2012, 3, 1),
    )
    mommy.make(
        "awards.Award",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
        description="small business administration",
        date_signed=date(2012, 3, 1),
    )
    mommy.make(
        "awards.Award",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
        description="SmaLL BusIness AdMinIstration",
        date_signed=date(2012, 3, 1),
    )
    mommy.make(
        "awards.Award",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
        description="Large BusIness AdMinIstration",
        date_signed=date(2012, 3, 1),
    )
    award = mommy.make(
        "awards.Award",
        piid="zzz",
        fain="small",
        type="B",
        total_obligation=1000,
        description="LARGE BUSINESS ADMINISTRATION",
        date_signed=date(2012, 3, 1),
    )

    mommy.make("awards.TransactionNormalized", description="Cool new tools", award=award)


@pytest.mark.django_db
def test_filter_generator_search_operation(client, mock_data):
    """Test search case insensitivity"""
    filters = [{"field": "description", "operation": "search", "value": "small"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 3

    resp = client.post(
        "/api/v1/awards/",
        content_type="application/json",
        data=json.dumps({"filters": [{"field": ["description", "fain"], "operation": "search", "value": "small"}]}),
    )
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data["results"]

    assert len(results) == 4


@pytest.mark.django_db
def test_filter_generator_in_operation(client, mock_data):
    """Test in operation case insensitivity"""
    filters = [
        {
            "field": "description",
            "operation": "in",
            "value": ["small business administration", "large business administration"],
        }
    ]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 5

    filters = [{"field": "description", "operation": "not_in", "value": ["small business administration", "large"]}]

    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 2


@pytest.mark.django_db
def test_filter_generator_equals_operation(client, mock_data):
    """Test equals case insensitivity"""
    filters = [{"field": "description", "operation": "equals", "value": "small business administration"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 3

    filters = [{"field": "description", "operation": "not_equals", "value": "small business administration"}]

    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 2


@pytest.mark.django_db
def test_filter_generator_fk_traversal(client, mock_data):
    """Test equals case insensitivity"""
    # Test FK filter
    filters = [{"field": "awarding_agency", "operation": "equals", "value": "10"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 1
    assert Award.objects.filter(q_obj).first().id == 1234

    # Test FK traversal filter
    filters = [{"field": "awarding_agency__toptier_agency", "operation": "equals", "value": "11"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 1
    assert Award.objects.filter(q_obj).first().id == 1234

    # Test FK traversal to string
    filters = [{"field": "awarding_agency__toptier_agency__name", "operation": "equals", "value": "LEXCORP"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 1
    assert Award.objects.filter(q_obj).first().id == 1234

    # Test fk traversal to string (Case-insensitive)
    filters = [{"field": "awarding_agency__toptier_agency__name", "operation": "equals", "value": "lexcorp"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 1
    assert Award.objects.filter(q_obj).first().id == 1234

    # Test lookup query - matching year on a month field
    filters = [{"field": "date_signed__year", "operation": "equals", "value": "2012"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 5


@pytest.mark.django_db
def test_filter_generator_reverse_fk(client, mock_data):
    filters = [{"field": "transactionnormalized__description", "operation": "search", "value": "cool"}]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 1
