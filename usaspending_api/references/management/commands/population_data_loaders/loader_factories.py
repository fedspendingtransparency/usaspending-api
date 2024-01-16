from usaspending_api.references.management.commands.population_data_loaders.loaders import (
    CountryPopulationLoader,
    DistrictPopulationLoader,
    Loader,
    CountyPopulationLoader,
)
from abc import ABC, abstractmethod
import logging


class PopulationLoaderFactory(ABC):
    """The creator class declares the factory method that must
    return an object of a Loaders class. The creator's subclasses
    usually choose which subclass of the Loader superclass to return.
    """

    @abstractmethod
    def create_population_loader(self) -> Loader:
        """Returns a population loader suitable for the use case at hand.

        Returns:
            Loader: An object of the type Loader that's a concrete implementation of the
            Loader class.
        """
        pass


class CountyPopulationLoaderFactory(PopulationLoaderFactory):
    def __init__(self):
        self._county_columns_mapper = {
            "state_code": "state_code",
            "county_code": "county_number",
            "state_name": "state_name",
            "county_name": "county_name",
            "population": "latest_population",
        }

    def create_population_loader(self):
        logger = logging.getLogger("script")
        loader = CountyPopulationLoader(self._county_columns_mapper, logger)
        return loader


class DistrictPopulationLoaderFactory(PopulationLoaderFactory):
    def __init__(self):
        self._district_columns_mapper = {
            "state_code": "state_code",
            "state_name": "state_name",
            "state_abbreviation": "state_abbreviation",
            "congressional_district": "congressional_district",
            "population": "latest_population",
        }

    def create_population_loader(self):
        logger = logging.getLogger("script")
        loader = DistrictPopulationLoader(self._district_columns_mapper, logger)
        return loader


class CountryPopulationLoaderFactory(PopulationLoaderFactory):
    def create_population_loader(self):
        logger = logging.getLogger("script")
        loader = CountryPopulationLoader(None, logger)
        return loader
