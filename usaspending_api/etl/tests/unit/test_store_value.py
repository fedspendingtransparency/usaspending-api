from usaspending_api.etl.management.load_base import store_value

def test_store_basic_values():
    testDict = {'x':3,'y':6}
    store_value(testDict,'x',2)
    assert testDict['x'] == 2
    assert testDict['y'] == 6
