import pytest

from django.conf import settings
from pyspark.sql import SparkSession


from usaspending_api.etl.elasticsearch_loader_helpers.controller_for_spark import (
    DeltaLakeElasticsearchIndexerController,
)


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


@pytest.fixture
def controller(spark):
    return DeltaLakeElasticsearchIndexerController({}, spark)


class TestDeltaLakeEsIndexerController:

    def test_ensure_view_exists(self, controller):
        controller.ensure_view_exists(settings.ES_LOCATIONS_ETL_VIEW_NAME)
