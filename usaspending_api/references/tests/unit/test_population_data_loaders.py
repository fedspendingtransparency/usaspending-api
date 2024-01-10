from unittest.mock import patch, MagicMock

import pytest
from usaspending_api.references.management.commands.population_data_loaders.loaders import (
    CountryPopulationLoader,
    CountyPopulationLoader,
    DistrictPopulationLoader,
    GenericPopulationLoader,
)


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_generic_population_loader_drop_temp_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    logger = MagicMock()
    loader = GenericPopulationLoader(column_mapper, logger)
    loader.drop_temp_tables()
    cursor_mock.execute.assert_called_with(f"DROP TABLE IF EXISTS {GenericPopulationLoader.TEMP_TABLE_NAME}")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_country_population_loader_drop_temp_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    logger = MagicMock()
    loader = CountryPopulationLoader(column_mapper, logger)
    loader.drop_temp_tables()
    cursor_mock.execute.call_count == 0


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_county_population_loader_drop_temp_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    logger = MagicMock()
    loader = CountyPopulationLoader(column_mapper, logger)
    loader.drop_temp_tables()
    cursor_mock.execute.assert_called_with(f"DROP TABLE IF EXISTS {GenericPopulationLoader.TEMP_TABLE_NAME}")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_district_population_loader_drop_temp_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    logger = MagicMock()
    loader = DistrictPopulationLoader(column_mapper, logger)
    loader.drop_temp_tables()
    cursor_mock.execute.assert_called_with(f"DROP TABLE IF EXISTS {GenericPopulationLoader.TEMP_TABLE_NAME}")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_generic_population_loader_create_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    test_data_cols = ["test"]
    logger = MagicMock()
    loader = GenericPopulationLoader(column_mapper, logger)
    loader.create_tables(test_data_cols)
    cursor_mock.execute.assert_called_with(f"CREATE TABLE {GenericPopulationLoader.TEMP_TABLE_NAME} (test TEXT);")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_county_population_loader_create_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    test_data_cols = ["test"]
    logger = MagicMock()
    loader = CountyPopulationLoader(column_mapper, logger)
    loader.create_tables(test_data_cols)
    cursor_mock.execute.assert_called_with(f"CREATE TABLE {GenericPopulationLoader.TEMP_TABLE_NAME} (test TEXT);")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_country_population_loader_create_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    test_data_cols = ["test"]
    logger = MagicMock()
    loader = CountryPopulationLoader(column_mapper, logger)
    loader.create_tables(test_data_cols)
    cursor_mock.execute.call_count == 0


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
def test_district_population_loader_create_tables(connection_mock):
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    column_mapper = MagicMock()
    test_data_cols = ["test"]
    logger = MagicMock()
    loader = DistrictPopulationLoader(column_mapper, logger)
    loader.create_tables(test_data_cols)
    cursor_mock.execute.assert_called_with(f"CREATE TABLE {GenericPopulationLoader.TEMP_TABLE_NAME} (test TEXT);")


@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.connection")
@patch("usaspending_api.references.management.commands.population_data_loaders.loaders.RefCountryCode")
def test_country_population_loader_load_data_missing_countries_error(django_model_mock, connection_mock):
    django_model_mock.__name__ = "RefCountryCode"
    django_model_mock.objects.all.return_value.distinct.return_value = ["USA", "CAN", "MEX"]
    column_mapper = MagicMock()
    test_data = [{"country_code": "USA"}]
    logger = MagicMock()
    loader = CountryPopulationLoader(column_mapper, logger)
    with pytest.raises(RuntimeError) as err:
        loader.load_data(data=test_data)
    assert (
        str(err.value)
        == "The provided data contains less than 50% of the known countries. We require at least 50% of the known countries to be present in the file to load. Data had 1 countries when there are 3 countries in RefCountryCode."
    )
