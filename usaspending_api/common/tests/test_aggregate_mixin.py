import datetime
from decimal import Decimal
from itertools import cycle
from operator import itemgetter
from unittest.mock import Mock

import pytest
from model_mommy.recipe import Recipe

from usaspending_api.awards.models import Award, Procurement
from usaspending_api.common.mixins import AggregateQuerysetMixin


@pytest.fixture()
def aggregate_models():
    """Set up data for testing aggregate functions."""
    award_uri = [None, None, 'yo']
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

    award_recipe.make(_quantity=4)
    award_list = [a for a in Award.objects.all()]

    # data to use for test transactions
    txn_amounts = [1000, None, 1000, 1000.02]
    award_types = ['U', 'B', '05', 'B']
    txn_action_dates = [
        datetime.date(2016, 7, 13),
        datetime.date(2017, 1, 1),
        datetime.date(2018, 6, 1,),
        datetime.date(2018, 1, 1,),
    ]
    pop_start_dates = txn_action_dates
    certified_dates = txn_action_dates
    pop_end_dates = [
        datetime.date(2018, 12, 31),
        datetime.date(2020, 1, 1),
        datetime.date(2050, 7, 14),
        datetime.date(2050, 7, 14)
    ]

    txn_recipe = Recipe(
        Procurement,  # change this to Transaction when the backend refactor is done
        action_date=cycle(txn_action_dates),
        type=cycle(award_types),
        federal_action_obligation=cycle(txn_amounts),
        period_of_performance_start_date=cycle(pop_start_dates),
        period_of_performance_current_end_date=cycle(pop_end_dates),
        # include a field that's not on the model's default field list
        certified_date=cycle(certified_dates),
        award=cycle(award_list)
    )

    # create test awards and related transactions for each of them
    # for award in Award.objects.all():
    txn_recipe.make(_quantity=16)


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
@pytest.mark.parametrize('model, request_data, result', [
    (Award, {'field': 'total_obligation', 'group': 'period_of_performance_start_date'}, [
        {'item': datetime.date(2017, 1, 1), 'aggregate': None},
        {'item': datetime.date(2018, 6, 1), 'aggregate': Decimal('4000.00')},
        {'item': datetime.date(2018, 1, 1), 'aggregate': Decimal('4000.08')},
        {'item': datetime.date(2016, 7, 13), 'aggregate': Decimal('4000.00')}
    ]),
    (Award, {'field': 'total_obligation', 'group': 'period_of_performance_start_date', 'date_part': 'year'}, [
        {'item': 2016, 'aggregate': Decimal('4000.00')},
        {'item': 2017, 'aggregate': None},
        {'item': 2018, 'aggregate': Decimal('8000.08')}
    ])
])
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
