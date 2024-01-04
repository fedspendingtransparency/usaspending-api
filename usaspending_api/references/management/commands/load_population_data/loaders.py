from abc import ABC, abstractmethod
from django.db import connection
from logging import Logger
from typing import List
from django.db import models
from usaspending_api.references.models import PopCongressionalDistrict, PopCounty


class Loader(ABC):
    """An abstract base class that *should* be implemented and returned by load population data
    factories. This class defines a common interface that all objects returned by load population data factories
    share. It's one part of an overall factory design implementation. Ultimately, allowing different load behaviors
    depending on the use case.
    """

    @abstractmethod
    def drop_temp_tables(self) -> None:
        pass

    @abstractmethod
    def create_tables(self, columns: List[str]) -> None:
        pass

    @abstractmethod
    def load_data(self, data: List[dict], model: models = None) -> None:
        pass

    @abstractmethod
    def cleanup(self) -> None:
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

    def load_data(self, data, model):
        model = PopCongressionalDistrict
        super().load_data(data, model)

    def cleanup(self):
        self.drop_temp_tables()


class CountyPopulationLoader(GenericPopulationLoader):
    """A concrete implemention of the generic population loader. Suitable for loading
    county population data.
    """

    def load_data(self, data, model):
        model = PopCounty
        super().load_data(data, model)

    def cleanup(self):
        self.drop_temp_tables()
