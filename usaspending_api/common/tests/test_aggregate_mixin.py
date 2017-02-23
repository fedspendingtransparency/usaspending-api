import datetime
from decimal import Decimal
from itertools import cycle
from operator import itemgetter
from unittest.mock import Mock

import pytest
from model_mommy.recipe import Recipe

from usaspending_api.awards.models import Award
from usaspending_api.common.mixins import AggregateQuerysetMixin


@pytest.fixture()
def aggregate_models():
    """Set up data for testing aggregate functions."""
    award_uri = [None, None, 'yo']
    award_fain = [None, None, '123']
    award_piid = ['abc', 'def', None]

    award_types = ['U', 'B', '05', 'B']
    start_dates = [
        datetime.date(2016, 7, 13),
        datetime.date(2017, 1, 1),
        datetime.date(2018,
                      6,
                      1, ),
        datetime.date(2018,
                      1,
                      1, ),
    ]
    end_dates = [
        datetime.date(2018, 12, 31), datetime.date(2020, 1, 1),
        datetime.date(2050, 7, 14), datetime.date(2050, 7, 14)
    ]
    obligated_amts = [1000.01, 2000, None, 4000.02]

    # create awards
    award_recipe = Recipe(
        Award,
        piid=cycle(award_piid),
        fain=cycle(award_fain),
        uri=cycle(award_uri),
        type=cycle(award_types),
        period_of_performance_start_date=cycle(start_dates),
        period_of_performance_current_end_date=cycle(end_dates),
        total_obligation=cycle(obligated_amts), )

    award_recipe.make(_quantity=4)

    # make each award parent of the next
    parent = None
    for award in Award.objects.order_by('period_of_performance_start_date'):
        award.parent_award = parent


@pytest.mark.django_db
def test_agg_fields(monkeypatch, aggregate_models):
    """Test length and field names of aggregate query result."""
    request = Mock()
    request.query_params = {}
    request.data = {'field': 'total_obligation', 'group': 'type'}
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: Award.objects.all()
    agg = a.aggregate(request=request)

    # Test number of returned recrods
    assert agg.count() == 3

    # Query should return two field names: 'item' and 'aggregate'
    fields = agg.first().keys()
    assert len(fields) == 2
    assert 'aggregate' in fields
    assert 'item' in fields


@pytest.mark.django_db
@pytest.mark.parametrize('model, request_data, result', [(Award, {
    'field': 'total_obligation',
    'group': 'period_of_performance_start_date'
}, [{
    'item': datetime.date(2017, 1, 1),
    'aggregate': Decimal('2000.00')
}, {
    'item': datetime.date(2018, 6, 1),
    'aggregate': None
}, {
    'item': datetime.date(2018, 1, 1),
    'aggregate': Decimal('4000.02')
}, {
    'item': datetime.date(2016, 7, 13),
    'aggregate': Decimal('1000.01')
}]), (Award, {
    'field': 'total_obligation',
    'group': 'period_of_performance_start_date',
    'date_part': 'year'
}, [{
    'item': 2016,
    'aggregate': Decimal('1000.01')
}, {
    'item': 2017,
    'aggregate': Decimal('2000.00')
}, {
    'item': 2018,
    'aggregate': Decimal('4000.02')
}]), (Award, {
    'field': 'total_obligation',
    'group': 'period_of_performance_start_date',
    'date_part': 'month'
}, [{
    'item': 1,
    'aggregate': Decimal('6000.02')
}, {
    'item': 6,
    'aggregate': None
}, {
    'item': 7,
    'aggregate': Decimal('1000.01')
}]), (Award, {
    'field': 'total_obligation',
    'group': 'period_of_performance_start_date',
    'date_part': 'day'
}, [{
    'item': 1,
    'aggregate': Decimal('6000.02')
}, {
    'item': 13,
    'aggregate': Decimal('1000.01')
}])])
def test_aggregate(monkeypatch, aggregate_models, model, request_data, result):
    request = Mock()
    request.query_params = {}
    request.data = request_data
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: model.objects.all()
    agg = a.aggregate(request=request)

    agg_list = [a for a in agg]
    if 'order' not in request_data:
        # this isn't an 'order by' request, (i.e., we're not testing
        # the result order), so sort the actual and expected results
        # to ensure a good comparison
        agg_list.sort(key=itemgetter('item'))
        result.sort(key=itemgetter('item'))

    assert agg_list == result


_expected_fy_aggregated = [{
    'item': 2016,
    'aggregate': Decimal('1000.01')
}, {
    'item': 2017,
    'aggregate': Decimal('2000.00')
}, {
    'item': 2018,
    'aggregate': Decimal('4000.02')
}]


@pytest.mark.django_db
@pytest.mark.parametrize('model, request_data, expected', [(Award, {
    'field': 'total_obligation',
    'group': 'period_of_performance_start_date__fy'
}, _expected_fy_aggregated)])
def test_aggregate_fy(monkeypatch, aggregate_models, model, request_data,
                      expected):
    request = Mock()
    request.query_params = {}
    request.data = request_data
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: model.objects.all()
    agg = a.aggregate(request=request)

    agg_list = [a for a in agg]
    if 'order' not in request_data:
        # this isn't an 'order by' request, (i.e., we're not testing
        # the result order), so sort the actual and expected results
        # to ensure a good comparison
        agg_list.sort(key=itemgetter('item'))
        expected.sort(key=itemgetter('item'))

    assert agg_list == expected


_expected_parent_fy_aggregated = [{
    'item': None,
    'aggregate': Decimal('1000.01')
}, {
    'item': 2016,
    'aggregate': Decimal('2000.00')
}, {
    'item': 2017,
    'aggregate': Decimal('4000.02')
}]


# TODO: support applying __fy through a FK traversal
@pytest.mark.skip
@pytest.mark.django_db
@pytest.mark.parametrize('model, request_data, expected', [(Award, {
    'field': 'total_obligation',
    'group': 'parent_award__period_of_performance_start_date__fy'
}, _expected_fy_aggregated)])
def test_aggregate_fy_with_traversal(monkeypatch, aggregate_models, model,
                                     request_data, expected):
    request = Mock()
    request.query_params = {}
    request.data = request_data
    a = AggregateQuerysetMixin()
    a.get_queryset = lambda: model.objects.all()
    agg = a.aggregate(request=request)

    agg_list = [a for a in agg]
    if 'order' not in request_data:
        # this isn't an 'order by' request, (i.e., we're not testing
        # the result order), so sort the actual and expected results
        # to ensure a good comparison
        agg_list.sort(key=itemgetter('item'))
        expected.sort(key=itemgetter('item'))

    assert agg_list == expected
