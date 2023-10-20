from mock import MagicMock, Mock, patch
from pytest import raises
from usaspending_api.etl.management.commands.load_transactions_in_delta import Command


def _setup_spark_table_exists_mock(return_value):
    spark_mock = Mock()
    catalog_mock = Mock()
    spark_mock._jsparkSession.catalog.return_value = catalog_mock
    catalog_mock.tableExists.return_value = return_value
    return spark_mock


def _setup_spark_mock():
    # Weird mock to get around the 'with self.prepare_spark()' statement
    spark_mock = MagicMock()
    spark_mock.__enter__.return_value = None  # This can be any value
    return spark_mock


@patch("usaspending_api.etl.management.commands.load_transactions_in_delta.get_earliest_load_date", return_value=None)
@patch(
    "usaspending_api.etl.management.commands.load_transactions_in_delta.Command.prepare_spark",
    new_callable=_setup_spark_mock,
)
def test_delete_records_sql_throws_exception(patch_prepare_spark, patch_load_date):
    command = Command()
    # This is needed along with 'prepare_spark' being patched to get around spark issues
    command.spark = _setup_spark_table_exists_mock(return_value=False)
    test_options = {"spark_s3_bucket": "some_bucket", "no_initial_copy": False}

    test_options["etl_level"] = "award_id_lookup"
    with raises(Exception, match="Table: int.award_id_lookup does not exist."):
        command.handle(**test_options)

    test_options["etl_level"] = "transaction_id_lookup"
    with raises(Exception, match="Table: int.transaction_id_lookup does not exist."):
        command.handle(**test_options)

    test_options["etl_level"] = "transaction_fabs"
    with raises(Exception, match="Table: int.transaction_fabs does not exist."):
        command.handle(**test_options)

    test_options["etl_level"] = "transaction_fpds"
    with raises(Exception, match="Table: int.transaction_fpds does not exist."):
        command.handle(**test_options)

    test_options["etl_level"] = "awards"
    with raises(Exception, match="Table: int.awards does not exist."):
        command.handle(**test_options)
