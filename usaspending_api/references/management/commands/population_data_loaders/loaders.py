from abc import ABC, abstractmethod
from django.db import connection
from logging import Logger
from typing import List
from django.db import models
from usaspending_api.references.models import PopCongressionalDistrict, PopCounty
from usaspending_api.references.models.ref_country_code import RefCountryCode


class Loader(ABC):
    """An abstract base class that *should* be implemented and returned by load population data
    factories. This class defines a common interface that all objects returned by load population data factories
    share. It's one part of an overall factory design implementation. Ultimately, allowing different load behaviors
    depending on the use case.
    """

    @abstractmethod
    def drop_temp_tables(self) -> None:
        """Drops any tables that were temporarily created to support
        this data load.
        """
        pass

    @abstractmethod
    def create_tables(self, columns: List[str]) -> None:
        """Creates any tables that are needed to support this data load.

        Args:
            columns: Columns defines the columns that will be loaded to.
        """
        pass

    @abstractmethod
    def load_data(self, data: List[dict], model: models = None) -> None:
        """Handles the loading of population data.

        Args:
            data: The population data that will be loaded.
            model: The model that defines the population object
                that this population loader will load. Defaults to None.
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Cleans up any tables or objects that were temporarily created to support
        this data load.
        """
        pass


class GenericPopulationLoader(Loader):
    """This class is a generic population loader that implements *some* of the
    loader interface. This class defines generic, common, behavior of the
    loader interface. Customize it, through inheritance, as needed.
    """

    TEMP_TABLE_NAME = "temp_population_load"
    TEMP_TABLE_SQL = "CREATE TABLE {table} ({columns});"

    def __init__(self, column_mapper, logger: Logger):
        self._columns_mapper = column_mapper
        self._logger = logger

    def drop_temp_tables(self):
        self._logger.info(f"Dropping temp table {self.TEMP_TABLE_NAME}")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEMP_TABLE_NAME}")

    def create_tables(self, columns):
        self._logger.info(f"Creating temp table {self.TEMP_TABLE_NAME}")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEMP_TABLE_NAME}")
            cursor.execute(
                self.TEMP_TABLE_SQL.format(table=self.TEMP_TABLE_NAME, columns=",".join([f"{c} TEXT" for c in columns]))
            )

    def load_data(self, data, model=None):
        self._logger.info(f"Attempting to load {len(data)} records into {model.__name__}")
        model.objects.all().delete()
        model.objects.bulk_create(
            [model(**{col: row[csv] for csv, col in self._columns_mapper.items()}) for row in data]
        )
        self._logger.info("Success? Please Verify")

    def cleanup(self):
        self.drop_temp_tables()


class DistrictPopulationLoader(GenericPopulationLoader):
    """A concrete implemention of the generic population loader. Suitable for loading
    district population data.
    """

    def load_data(self, data, model=None):
        model = PopCongressionalDistrict
        super().load_data(data, model)


class CountyPopulationLoader(GenericPopulationLoader):
    """A concrete implemention of the generic population loader. Suitable for loading
    county population data.
    """

    def load_data(self, data, model=None):
        model = PopCounty
        super().load_data(data, model)


class CountryPopulationLoader(GenericPopulationLoader):
    """A concrete implemention of the generic population loader. Suitable for loading
    country population data.
    """

    ERROR_THRESHOLD = 0.5

    def load_data(self, data, model=None):
        model = RefCountryCode
        # Ensuring that the update will result under our accepted error threshold
        total_countries = model.objects.all().distinct("country_code")
        if float(len(data)) / float(len(total_countries)) < self.ERROR_THRESHOLD:
            raise RuntimeError(
                "The provided data contains less than 50% of the known countries. "
                "We require at least 50% of the known countries to be present in the file to load. "
                f"Data had {len(data)} countries when there are {len(total_countries)} countries in {model.__name__}."
            )
        self._logger.info(f"Attempting to load {len(data)} records into {model.__name__}")

        for row in data:
            record = model.objects.get(country_code=row["country_code"])
            data_file_population = row["population"]
            # Only update the update_date attribute of this record when the population value actually changes
            # This class assumes that update_date has the auto_now property set to True
            # by checking if the population value changes and only saving the record when it does
            # we take advantage of the knowledge of an external class to ensure that the update_date
            # value doesn't change.
            if str(record.latest_population) != str(data_file_population):
                record.latest_population = data_file_population
                record.save()
        self._logger.info("Success? Please Verify")

    def drop_temp_tables(self):
        # This implementation is purely an update, no need to drop temp tables
        pass

    def create_tables(self, columns):
        # This implementation is purely an update, no need to create temp tables
        pass

    def cleanup(self):
        # This implementation is purely an update, no need to cleanup temp tables
        pass
