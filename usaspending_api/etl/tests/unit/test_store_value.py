from usaspending_api.etl.management.load_base import store_value
import re

def test_store_basic_values():
    test_dict = {'x':3,'y':6}
    store_value(test_dict,'x',2)
    assert test_dict['x'] == 2
    assert test_dict['y'] == 6

def test_store_none():
    test_dict = {'x': 3, 'y': 6}
    store_value(test_dict, 'x', None)
    assert test_dict['x'] is None
    assert test_dict['y'] == 6

def test_store_valid_dates():
    test_dict = {'test_date': 3, 'y': 6}
    good_date = '2005-08-31 00:00:00'
    store_value(test_dict, 'test_date', good_date)
    assert str(test_dict['test_date']) == '2005-08-31'
    assert test_dict['y'] == 6

def test_store_unlabeled_dates():
    test_dict = {'test_date_not': 3, 'y': 6}
    good_date = '2005-08-31 00:00:00'
    store_value(test_dict, 'test_date_not', good_date)
    assert test_dict['test_date_not'] == '2005-08-31 00:00:00'
    assert test_dict['y'] == 6

def test_store_bad_dates():
    test_dict = {'test_bad_date': 3, 'y': 6}
    bad_date = [4,7]
    store_value(test_dict, 'test_date_not', bad_date)
    assert test_dict['test_date_not'] == [4,7]
    assert test_dict['y'] == 6