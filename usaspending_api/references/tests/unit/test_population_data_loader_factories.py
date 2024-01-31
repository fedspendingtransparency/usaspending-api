from unittest.mock import patch, MagicMock
import unittest
from usaspending_api.references.management.commands.population_data_loaders.loader_factories import (
    CountryPopulationLoaderFactory,
    CountyPopulationLoaderFactory,
    DistrictPopulationLoaderFactory,
)
from usaspending_api.references.management.commands.population_data_loaders.loaders import (
    CountryPopulationLoader,
    CountyPopulationLoader,
    DistrictPopulationLoader,
)


class TestLoaderFactories(unittest.TestCase):
    @patch("usaspending_api.references.management.commands.population_data_loaders.loader_factories.logging")
    def test_county_population_loader_factory(self, logging_mock):
        logging_mock.get_logger = MagicMock()
        loader_factory = CountyPopulationLoaderFactory()
        loader = loader_factory.create_population_loader()
        self.assertIsInstance(loader, CountyPopulationLoader)

    @patch("usaspending_api.references.management.commands.population_data_loaders.loader_factories.logging")
    def test_country_population_loader_factory(self, logging_mock):
        logging_mock.get_logger = MagicMock()
        loader_factory = CountryPopulationLoaderFactory()
        loader = loader_factory.create_population_loader()
        self.assertIsInstance(loader, CountryPopulationLoader)

    @patch("usaspending_api.references.management.commands.population_data_loaders.loader_factories.logging")
    def test_district_population_loader_factory(self, logging_mock):
        logging_mock.get_logger = MagicMock()
        loader_factory = DistrictPopulationLoaderFactory()
        loader = loader_factory.create_population_loader()
        self.assertIsInstance(loader, DistrictPopulationLoader)
