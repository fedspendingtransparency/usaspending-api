from abc import ABC, abstractmethod
from django.db import connection
from logging import Logger
from typing import List
from django.db import models
from usaspending_api.references.models import PopCongressionalDistrict, PopCounty

class Loader(ABC):
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


class CountyPopulationLoaderFactory:
    def __init__(self):
        self._county_columns_mapper = {
            "state_code": "state_code",
            "county_code": "county_number",
            "state_name": "state_name",
            "county_name": "county_name",
            "population": "latest_population",
        }


class DistrictPopulationLoaderFactory:
    def __init__(self):
        self._county_columns_mapper = {
            "state_code": "state_code",
            "state_name": "state_name",
            "state_abbreviation": "state_abbreviation",
            "congressional_district": "congressional_district",
            "population": "latest_population",
        }


class GenericPopulationLoader(Loader):
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

    def load_data(self, data, model = None):
        self._logger.info(f"Attempting to load {len(data)} records into {model.__name__}")
        model.objects.all().delete()
        model.objects.bulk_create([model(**{col: row[csv] for csv, col in self._columns_mapper.items()}) for row in data])
        self._logger.info("Success? Please Verify")

    def cleanup(self):
        self.drop_temp_tables()

class DistrictPopulationLoader(GenericPopulationLoader):

    def load_data(self, data, model):
        model = PopCongressionalDistrict
        super().load_data(data, model)

    def cleanup(self):
        self.drop_temp_tables()

class CountyPopulationLoader(GenericPopulationLoader):

    def load_data(self, data, model):
        model = PopCounty
        super().load_data(data, model)

    def cleanup(self):
        self.drop_temp_tables()
