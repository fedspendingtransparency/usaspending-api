from usaspending_api.etl.transaction_loaders.derived_field_functions_fpds import calculate_fiscal_year


def test_calculate_fiscal_year():
    """Check that calculate can derive a year from the structure of broker data passed in

    Just checking that a year is retrieved.
    It's up to unit tests of the `fy` function to ensure it's logic is correct
    """
    dummy_broker_data = {"action_date": "2019-04-18 00:00:00"}
    fy = calculate_fiscal_year(dummy_broker_data)
    assert isinstance(fy, int)
    assert 2018 <= fy <= 2020
