from mock import Mock
from pytest import raises
from usaspending_api.etl.management.commands.load_transactions_in_delta import Command


def setup_spark_table_exists_mock(return_value):
    spark_mock = Mock()
    catalog_mock = Mock()
    spark_mock._jsparkSession.catalog.return_value = catalog_mock
    catalog_mock.tableExists.return_value = return_value
    return spark_mock


def test_award_id_lookup_pre_delete_throws_exception():
    command_mock = Mock()
    command_mock.spark = setup_spark_table_exists_mock(return_value=False)

    command_mock.etl_level = "award_id_lookup"
    with raises(Exception, match="Table: int.award_id_lookup does not exist."):
        Command.delete_records_sql(command_mock)


def test_delete_records_sql_throws_exception():
    command_mock = Mock()
    command_mock.spark = setup_spark_table_exists_mock(return_value=False)

    command_mock.etl_level = "award_id_lookup"
    with raises(Exception, match="Table: int.award_id_lookup does not exist."):
        Command.delete_records_sql(command_mock)

    command_mock.etl_level = "transaction_id_lookup"
    with raises(Exception, match="Table: int.transaction_id_lookup does not exist."):
        Command.delete_records_sql(command_mock)

    command_mock.etl_level = "transaction_fabs"
    with raises(Exception, match="Table: int.transaction_fabs does not exist."):
        Command.delete_records_sql(command_mock)

    command_mock.etl_level = "transaction_fpds"
    with raises(Exception, match="Table: int.transaction_fpds does not exist."):
        Command.delete_records_sql(command_mock)
