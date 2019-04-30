from usaspending_api.etl.management.load_base import store_value

def test_store_basic_values():
    testDict = {'x':3,'y':6}
    store_value(testDict,'x',2)
    assert testDict['x'] == 2
    assert testDict['y'] == 6

def test_store_valid_dates():
    testDict = {'test_date': 3, 'y': 6}
    goodDate = '2005-08-31 00:00:00'
    store_value(testDict, 'test_date', goodDate)
    assert testDict['x'] == '2005-08-31'
    assert testDict['y'] == 6
