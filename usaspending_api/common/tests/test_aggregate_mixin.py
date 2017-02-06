import datetime
from decimal import Decimal
from itertools import cycle
import pytest
from unittest.mock import Mock

from model_mommy.recipe import Recipe

from usaspending_api.awards.models import Award, Procurement
from usaspending_api.common.mixins import AggregateQuerysetMixin


@pytest.fixture()
def aggregate_models():
    """Set up data for testing aggregate functions."""
    award_uri = [None, None, 'haha']
    award_fain = [None, None, '123']
    award_piid = ['abc', 'def', None]

    # create awards. note that many award fields (e.g., type,
    # period_of_performance dates) will be set automatically
    # as corresponding transactions are created)
    award_recipe = Recipe(
        Award,
        piid=cycle(award_piid),
        fain=cycle(award_fain),
        uri=cycle(award_uri),
    )

    award_recipe.make(_quantity=3)
    award_list = [a for a in Award.objects.all()]

    # data to use for test transactions
    txn_amounts = [1000, 1000.02, 1000]
    award_types = ['U', 'B', '05']
    txn_action_dates = [
        datetime.date(2016, 7, 13),
        datetime.date(2017, 1, 1),
        datetime.date(2018, 1, 1,),
    ]
    pop_start_dates = txn_action_dates
    pop_end_dates = [
        datetime.date(2018, 12, 31),
        datetime.date(2020, 1, 1),
        datetime.date(2050, 7, 14)
    ]

    txn_recipe = Recipe(
        Procurement,  # change this to Transaction when the backend refactor is done
        action_date=cycle(txn_action_dates),
        type=cycle(award_types),
        federal_action_obligation=cycle(txn_amounts),
        period_of_performance_start_date=cycle(pop_start_dates),
        period_of_performance_current_end_date=cycle(pop_end_dates),
        award=cycle(award_list)
    )

    # create test awards and related transactions for each of them
    # for award in Award.objects.all():
    txn_recipe.make(_quantity=6)


@pytest.mark.django_db
def test_sum(monkeypatch, aggregate_models):
    """Test aggregation by year (using the default aggreagtion of SUM)."""
    request = Mock()
    request.query_params = {}
    request.data = {'field': 'federal_action_obligation', 'group': 'action_date'}
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: Procurement.objects.all()

    agg = a.aggregate(request=request)

    assert agg.count() == 3
    single_result = agg.get(item=datetime.date(2016, 7, 13))
    fields = single_result.keys()

    # make sure aggregate query returns expected field names
    assert len(fields) == 2
    assert 'aggregate' in fields
    assert 'item' in fields

    # test returned aggregate values
    assert single_result['aggregate'] == 2000
    assert agg.get(item=datetime.date(2017, 1, 1))['aggregate'] == Decimal('2000.04')
    assert agg.get(item=datetime.date(2018, 1, 1))['aggregate'] == 2000


@pytest.mark.django_db
def test_sum_by_year(monkeypatch, aggregate_models):
    """Test SUMing by the year of a date field."""
    request = Mock()
    request.query_params = {}
    request.data = {
        'field': 'total_obligation',
        'group': 'period_of_performance_start_date',
        'date_part': 'year'}
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: Award.objects.all()

    agg = a.aggregate(request=request)

    assert agg.count() == 3
    single_result = agg.get(item=2016)
    fields = single_result.keys()

    # make sure aggregate query returns expected field names
    assert len(fields) == 2
    assert 'aggregate' in fields
    assert 'item' in fields
    assert single_result['item'] == 2016

    # test returned aggregate values
    assert single_result['aggregate'] == 2000
    assert agg.get(item=2017)['aggregate'] == Decimal('2000.04')
    assert agg.get(item=2018)['aggregate'] == 2000


@pytest.mark.django_db
def test_sum_by_month(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_by_day(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_by_fk(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_by_year_fk(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_no_grouping(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_by_multiple_fk(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_by_non_default_fields(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_sum_ordering(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_avg_fk(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_count_ordering(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_max_no_group(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_min_by_multiple_fk(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_min_with_null(monkeypatch, aggregate_models):
    pass


@pytest.mark.django_db
def test_bad_agg_request(monkeypatch, aggregate_models):
    pass
