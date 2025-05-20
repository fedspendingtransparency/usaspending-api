"""
Boilerplate helper functions for setup and configuration of the Spark environment

NOTE: This is distinguished from the usaspending_api.common.etl.spark module, which holds Spark utility functions that
could be used as stages or steps of an ETL job (aka "data pipeline")
"""

import inspect
import logging
import os
import sys
import re

from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Set, Union

from django.core.management import call_command
from py4j.java_gateway import (
    JavaGateway,
)
from py4j.protocol import Py4JJavaError
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.find_spark_home import _find_spark_home
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import read_int, UTF8Deserializer
from pyspark.sql import SparkSession

from usaspending_api.awards.delta_models.awards import AWARDS_COLUMNS
from usaspending_api.awards.delta_models.financial_accounts_by_awards import FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS
from usaspending_api.common.helpers.aws_helpers import is_aws, get_aws_credentials
from usaspending_api.config import CONFIG
from usaspending_api.config.utils import parse_pg_uri, parse_http_url
from usaspending_api.transactions.delta_models import DETACHED_AWARD_PROCUREMENT_DELTA_COLUMNS, PUBLISHED_FABS_COLUMNS
from usaspending_api.transactions.delta_models.transaction_fabs import (
    TRANSACTION_FABS_COLUMN_INFO,
    TRANSACTION_FABS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    TRANSACTION_FPDS_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS

logger = logging.getLogger(__name__)


def get_active_spark_context() -> Optional[SparkContext]:
    """Returns the active ``SparkContext`` if there is one and it's not stopped, otherwise returns None"""
    if is_spark_context_stopped():
        return None
    return SparkContext._active_spark_context


def get_active_spark_session() -> Optional[SparkSession]:
    """Returns the active ``SparkSession`` if there is one and it's not stopped, otherwise returns None"""
    if is_spark_context_stopped():
        return None
    return SparkSession.getActiveSession()


def is_spark_context_stopped() -> bool:
    is_stopped = True
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        if SparkContext._active_spark_context is not None:
            sc = SparkContext._active_spark_context
            is_stopped = not (sc._jvm and not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped())
    return is_stopped


def stop_spark_context() -> bool:
    stopped_without_error = True
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        if SparkContext._active_spark_context is not None:
            sc = SparkContext._active_spark_context
            if (
                sc._jvm
                and hasattr(sc._jvm, "SparkSession")
                and sc._jvm.SparkSession
                and not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()
            ):
                try:
                    sc.stop()
                except Exception:
                    # Swallow errors if not able to stop (e.g. may have already been stopped)
                    stopped_without_error = False
    return stopped_without_error


def configure_spark_session(
    java_gateway: JavaGateway = None,
    spark_context: Union[SparkContext, SparkSession] = None,
    master=None,
    app_name="Spark App",
    log_level: int = None,
    log_spark_config_vals: bool = False,
    log_hadoop_config_vals: bool = False,
    enable_hive_support: bool = False,
    **options,
) -> SparkSession:
    """Get a SparkSession object with some of the default/boiler-plate config needed for THIS project pre-set

    Providing no arguments will work, and give a plain-vanilla SparkSession wrapping a plain vanilla SparkContext
    with all the default spark configurations set (or set with any file-based configs that have been established in
    the runtime environment (e.g. $SPARK_HOME/spark-defaults.conf)

    Use arguments in varying combinations to override or provide pre-configured components of the SparkSession.
    Lastly, provide a dictionary or exploded dict (like: **my_options) of name-value pairs of spark properties with
    specific values.

    Args:
        java_gateway (JavaGateway): Provide your own JavaGateway, which is typically a network interface to a running
            spark-submit process, through which PySpark jobs can be submitted to a JVM-based Spark runtime.
            NOTE: Only JavaGateway and not ClientServer (which would be used for support of PYSPARK_PIN_THREAD) is
            supported at this time.

        spark_context (SparkContext): Provide your own pre-built or fetched-from-elsewhere SparkContext object that
            the built SparkSession will wrap. The given SparkContext must be active (not stopped). Since an active
            SparkContext will have its own active underlying JVM gateway, you cannot provide this AND a java_gateway.

        master (str): URL in the form of spark://host:port where the master node of the Spark cluster can be found.
            If not provided here, or via a conf property spark.master, the default value of local[*] will remain.

        app_name (str): The name given to the app running in this SparkSession. This is not a modifiable property,
            and can only be set if creating a brand new SparkContext and SparkSession.

        log_level (str): Set the log level. Only set AFTER construction of the SparkContext, unfortunately.
            Values are one of: logging.ERROR, logging.WARN, logging.WARNING, logging.INFO, logging.DEBUG

        log_spark_config_vals (bool): If True, log at INFO the current spark config property values

        log_hadoop_config_vals (bool): If True, log at INFO the current hadoop config property values

        enable_hive_support (bool): If True, enable hive on the created SparkSession. Doing so internally sets the
            spark conf spark.sql.catalogImplementation=hive, which persists the metastore_db to its configured location
            (by default the working dir of the spark command run as a Derby DB folder, but configured explicitly here
            if running locally (not AWS))

        options (kwargs): dict or named-arguments (unlikely due to dots in properties) of key-value pairs representing
            additional spark config values to set as the SparkContext and SparkSession are created.
            NOTE: If a value is provided, and a SparkContext is also provided, the value must be a modifiable
            property, otherwise an error will be thrown.
    """
    if spark_context and (
        not spark_context._jvm or spark_context._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()
    ):
        raise ValueError("The provided spark_context arg is a stopped SparkContext. It must be active.")
    if spark_context and java_gateway:
        raise Exception(
            "Cannot provide BOTH spark_context and java_gateway args. The active spark_context supplies its own gateway"
        )

    conf = SparkConf()

    # Normalize all timestamps read into the SparkSession to UTC time.
    # So if timezone-aware timestamps are read-in, Spark will shifted to UTC and then strip off the timezone so they
    # are only an "instant" (no timezone part)
    # - See also: https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#timestamps-and
    #   time-zones
    # - if not set, it will fallback to the timezone of the JVM running spark, which could be the local time zone of
    #   the machine
    #   - Still would be ok so long as reads and writes of an "instant" happen from the same session timezone
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.scheduler.mode", CONFIG.SPARK_SCHEDULER_MODE)
    # Don't try to re-run the whole job if there's an error
    # Assume that random errors are rare, and jobs have long runtimes, so fail fast, fix and retry manually.
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.hadoop.fs.s3a.endpoint", CONFIG.AWS_S3_ENDPOINT)

    if not CONFIG.USE_AWS:  # i.e. running in a "local" [development] environment
        # Set configs to allow the S3AFileSystem to work against a local MinIO object storage proxy
        conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # "Enable S3 path style access ie disabling the default virtual hosting behaviour.
        # Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting."
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

        # Documenting for Awareness:
        # Originally it was thought that the S3AFileSystem "Committer" needed config changes to be compliant when
        # hitting MinIO locally instead of AWS S3 service. However, those changs were proven unnecessary.
        # - There is however an intermitten issue which we cannot quite identify the root cause (perhaps changing
        #   back to defaults will keep it from happening again). See: https://github.com/minio/minio/issues/10744
        #   - The error comes back as "FileAlreadyExists" ... or just: "<file/folder> already exists"
        #   - It is either some cache purging of Docker or MinIO or both over time that fixes it
        #   - Or some fiddling with these committer settings
        # - For Committer Details: https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html
        # - Findings:
        #   - If USE_AWS = True, and you point it at an AWS bucket...
        #     - s3a.path.style.access=true works, by itself as well as with no committer specified (falls back to
        #      FileOutputCommitter) and any combo of conflict-mode and tmp.path
        #     - However if committer.name="directory" (it uses the StagingOutputCommitter) AND conflict-mode=replace,
        #       it will replace the whole directory at the last file write, which is the _SUCCESS file, and that's why
        #       that's the only file you see
        # - The below settings are the DEFAULT when not set, but documenting here FYI
        # conf.set("spark.hadoop.fs.s3a.committer.name", "file")
        # conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "fail")
        # conf.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "tmp/staging")

        # Turn on Hive support to use a Derby filesystem DB as the metastore DB for tracking of schemas and tables
        enable_hive_support = True

        # Add Spark conf to set the Spark SQL Warehouse to an explicit directory,
        # and to make the Hive metastore_db folder get stored under that warehouse dir
        conf.set("spark.sql.warehouse.dir", CONFIG.SPARK_SQL_WAREHOUSE_DIR)
        conf.set(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={CONFIG.HIVE_METASTORE_DERBY_DB_DIR};create=true",
        )

    # Set AWS credentials in the Spark config
    # Hint: If connecting to AWS resources when executing program from a local env, and you usually authenticate with
    # an AWS_PROFILE, set each of these config values to empty/None, and ensure your AWS_PROFILE env var is set in
    # the shell when executing this program, and set temporary_creds=True.
    configure_s3_credentials(
        conf,
        CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        CONFIG.AWS_SECRET_KEY.get_secret_value(),
        CONFIG.AWS_PROFILE,
        temporary_creds=False,
    )

    # Set optional config key=value items passed in as args
    # Do this after all required config values are set with their defaults to allow overrides by passed-in values
    [conf.set(str(k), str(v)) for k, v in options.items() if options]

    # NOTE: If further configuration needs to be set later (after SparkSession is built), use the below, where keys are
    # not prefixed with "spark.hadoop.", e.g.:
    # spark.sparkContext._jsc.hadoopConfiguration().set("key", value), e.g.
    # spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    # EXPLORE THE ABOVE WITH CAUTION. It may not stick, and if it does, it's generally dangerous to modify a
    #     JavaSparkContext (_jsc) since all SparkSessions share that single context and its config

    # Build the SparkSession based on args provided
    builder = SparkSession.builder
    if spark_context:
        builder = builder._sparkContext(spark_context)
    if java_gateway:
        SparkContext._ensure_initialized(gateway=java_gateway, conf=conf)
        sc_with_gateway = SparkContext.getOrCreate(conf=conf)
        builder = builder._sparkContext(sc_with_gateway)
    if master:
        builder = builder.master(master)
    if app_name:
        builder = builder.appName(app_name)
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    spark = builder.config(conf=conf).getOrCreate()

    # Now that the SparkSession was created, check whether certain provided config values were ignored if given a
    # pre-existing SparkContext, and error-out if so
    if spark_context:
        built_conf = spark.conf
        provided_conf_keys = [item[0] for item in conf.getAll()]
        non_modifiable_conf = [k for k in provided_conf_keys if not built_conf.isModifiable(k)]
        if non_modifiable_conf:
            raise ValueError(
                "An active SparkContext was given along with NEW spark config values. The following "
                "spark config values were not set because they are not modifiable on the active "
                "SparkContext"
            )

    # Override log level, if provided
    # While this is a bit late (missing out on any logging at SparkSession instantiation time),
    # could not find a way (aside from injecting a ${SPARK_HOME}/conf/log4.properties file) to have it pick up
    # the desired log level at Spark startup time
    if log_level:
        logging._checkLevel(log_level)  # throws error if not recognized
        log_level_name = logging.getLevelName(log_level)
        if log_level_name == "WARNING":
            log_level_name = "WARN"  # tranlate to short-form used by log4j
        spark.sparkContext.setLogLevel(log_level_name)

    logger.info("PySpark Job started!")
    logger.info(
        f"""
@       Found SPARK_HOME: {_find_spark_home()}
@       Python Version: {sys.version}
@       Spark Version: {spark.version}
@       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
    """
    )
    es_config = get_es_config()
    es_auth = ""
    if "es.net.http.auth.user" in es_config and "es.net.http.auth.pass" in es_config:
        es_auth = f"{es_config['es.net.http.auth.user']}:********@"
    logger.info(
        f"Running Job with:\n"
        f"\tDB = {get_usas_jdbc_url().rsplit('=', 1)[0] + '=********'}"
        f"\n\tES = {'https' if (es_config['es.net.ssl'] and es_config['es.net.ssl'].lower() != 'false') else 'http'}://"
        f"{es_auth}{es_config['es.nodes']}:{es_config['es.port']}"
        f"\n\tS3 = {conf.get('spark.hadoop.fs.s3a.endpoint')} with "
        f"spark.hadoop.fs.s3a.access.key='{conf.get('spark.hadoop.fs.s3a.access.key')}' and "
        f"spark.hadoop.fs.s3a.secret.key='{'********' if conf.get('spark.hadoop.fs.s3a.secret.key') else ''}'"
    )

    if log_spark_config_vals:
        log_spark_config(spark)
    if log_hadoop_config_vals:
        log_hadoop_config(spark)
    return spark


def read_java_gateway_connection_info(gateway_conn_info_path):  # pragma: no cover -- useful development util
    """Read the port and auth token from a file holding connection info to a running spark-submit process

    Args:
        gateway_conn_info_path (path-like): File path of a file that the spun-up spark-submit process would have
            written its port and secret info to. In order to do so this file path would have needed to be provided in an
            environment variable named _PYSPARK_DRIVER_CONN_INFO_PATH in the environment where the spark-submit
            process was started. It will read that, and dump out its connection info to that file upon starting.
    """
    with open(gateway_conn_info_path, "rb") as conn_info:
        gateway_port = read_int(conn_info)
        gateway_secret = UTF8Deserializer().loads(conn_info)
    return gateway_port, gateway_secret


def attach_java_gateway(
    gateway_port,
    gateway_auth_token,
) -> JavaGateway:  # pragma: no cover -- useful development util
    """Create a new JavaGateway that latches onto the port of a running spark-submit process

    Args:
        gateway_port (int): Port on which the spark-submit process will allow the gateway to attach
        gateway_auth_token: Shared secret that must be provided to attach to the spark-submit process

    Returns: The instantiated JavaGateway, which acts as a network interface for PySpark to submit spark jobs through
        to the JVM-based Spark runtime
    """
    os.environ["PYSPARK_GATEWAY_PORT"] = str(gateway_port)
    os.environ["PYSPARK_GATEWAY_SECRET"] = gateway_auth_token

    gateway = launch_gateway()

    # ALTERNATIVE IMPL BELOW, THAT WOULD ALLOW SETTING THE IP ADDRESS WHERE THE JAVA GATEWAY CAN BE FOUND
    #     - HOWEVER APPEARS TO NOT WORK FROM OUTSIDE-IN OF A CONTAINER, PROBABLY DUE TO IT NOT BEING ABLE TO CALLBACK
    #       TO THE PYTHON PROCESS SINCE IT IS HARD-CODED TO LOOK AT LOCALHOST
    # gateway = JavaGateway(
    #     gateway_parameters=GatewayParameters(
    #         address=gateway_address,
    #         port=gateway_port,
    #         auth_token=gateway_auth_token,
    #         auto_convert=True))
    #
    # gateway.proc = None  # no self-started process, latching on to externally started gateway process
    #
    # # CAUTION: These imports were copied from pyspark/java_gateway.py -> launch_gateway(). They should be checked for
    # #          change if an error occurs
    #
    # # Import the classes used by PySpark
    # java_import(gateway.jvm, "org.apache.spark.SparkConf")
    # java_import(gateway.jvm, "org.apache.spark.api.java.*")
    # java_import(gateway.jvm, "org.apache.spark.api.python.*")
    # java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    # java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # java_import(gateway.jvm, "org.apache.spark.resource.*")
    # # TODO(davies): move into sql
    # java_import(gateway.jvm, "org.apache.spark.sql.*")
    # java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
    # java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    # java_import(gateway.jvm, "scala.Tuple2")

    return gateway


def get_jdbc_connection_properties(fix_strings=True) -> dict:
    jdbc_props = {"driver": "org.postgresql.Driver", "fetchsize": str(CONFIG.SPARK_PARTITION_ROWS)}
    if fix_strings:
        # This setting basically tells the JDBC driver how to process the strings, which could be a special type casted
        # as a string (ex. UUID, JSONB). By default, it assumes they are actually strings. Setting this to "unspecified"
        # tells the driver to not make that assumption and let the schema try to infer the type on insertion.
        # See the `stringtype` param on https://jdbc.postgresql.org/documentation/94/connect.html for details.
        jdbc_props["stringtype"] = "unspecified"
    return jdbc_props


def get_jdbc_url_from_pg_uri(pg_uri: str) -> str:
    """Converts the passed-in Postgres DB connection URI to a JDBC-compliant Postgres DB connection string"""
    url_parts, user, password = parse_pg_uri(pg_uri)
    if user is None or password is None:
        raise ValueError("pg_uri provided must have username and password with host or in query string")
    # JDBC URLs only support postgresql://
    pg_uri = f"postgresql://{url_parts.hostname}:{url_parts.port}{url_parts.path}?user={user}&password={password}"

    return f"jdbc:{pg_uri}"


def get_usas_jdbc_url():
    """Getting a JDBC-compliant Postgres DB connection string hard-wired to the POSTGRES vars set in CONFIG"""
    if not CONFIG.DATABASE_URL:
        raise ValueError("DATABASE_URL config val must provided")

    return get_jdbc_url_from_pg_uri(CONFIG.DATABASE_URL)


def get_broker_jdbc_url():
    """Getting a JDBC-compliant Broker Postgres DB connection string hard-wired to the POSTGRES vars set in CONFIG"""
    if not CONFIG.DATA_BROKER_DATABASE_URL:
        raise ValueError("DATA_BROKER_DATABASE_URL config val must provided")

    return get_jdbc_url_from_pg_uri(CONFIG.DATA_BROKER_DATABASE_URL)


def get_es_config():  # pragma: no cover -- will be used eventually
    """
    Get a base template of Elasticsearch configuration settings tailored to the specific environment setup being
    used

    NOTE that this is the base or template config. index-specific values should be overwritten in a copy of this
    config; e.g.
        base_config = get_es_config()
        index_config = base_config.copy()
        index_config["es.resource.write"] = name      # for index name
        index_config["es.mapping.routing"] = routing  # for index routing key
        index_config["es.mapping.id"] = doc_id        # for _id field of indexed documents
    """
    es_url = CONFIG.ES_HOSTNAME
    url_parts, url_username, url_password = parse_http_url(es_url)
    ssl = url_parts.scheme == "https"
    host = url_parts.hostname
    port = url_parts.port if url_parts.port else "443" if ssl else "80"
    user = url_username or ""
    password = url_password or ""

    # More values at:
    # - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    # - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html#spark-python
    config = {
        "es.resource.write": "",
        "es.nodes": host,
        "es.port": str(port),  # default 9200
        "es.index.auto.create": "yes",  # default yes
        # "es.mapping.id": "_id",  # defaults to not set
        # "es.nodes.data.only": "false",                    # default true, but should not be set when in WAN-only mode
        "es.nodes.wan.only": "true",  # default false
        "es.net.http.auth.user": user,  # default (not set). Set if running on a local cluster that has auth
        "es.net.http.auth.pass": password,  # default (not set) Set if running on a local cluster that has auth
        "es.net.ssl": str(ssl).lower(),  # default false
        "es.net.ssl.cert.allow.self.signed": "true",  # default false
        "es.batch.size.entries": str(CONFIG.ES_BATCH_ENTRIES),  # default 1000
        "es.batch.size.bytes": str(CONFIG.ES_MAX_BATCH_BYTES),  # default 1024*1024 (1mb)
        "es.batch.write.refresh": "false",  # default true, to refresh after configured batch size completes
    }

    if is_aws():
        # Basic auth only required for local clusters
        config.pop("es.net.http.auth.user")
        config.pop("es.net.http.auth.pass")

    return config


def get_jvm_logger(spark: SparkSession, logger_name=None):
    """
    Get a JVM log4j Logger object instance to log through Java

    WARNING about Logging: This is NOT python's `logging` module
    This is a python proxy to a java Log4J Logger object
    As such, you can't do everything with it you'd do in Python, NOTABLY: passing
    keyword args, like `logger.error("msg here", exc_info=exc)`. Instead do e.g.:
    `logger.error("msg here", exc)`
    Also, errors may not be loggable in the traditional way. See: https://www.py4j.org/py4j_java_protocol.html#
    `logger.error("msg here", exc)` should probably just format the stack track from Java:
    `logger.error("msg here:\n {str(exc)}")`
    """
    if not logger_name:
        try:
            calling_function_name = inspect.stack()[1][3]
            logger_name = calling_function_name
        except Exception:
            logger_name = "pyspark_job"
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(logger_name)
    return logger


def log_java_exception(logger, exc, err_msg=""):
    if exc and (isinstance(exc, Py4JJavaError) or hasattr(exc, "java_exception")):
        logger.error(f"{err_msg}\n{str(exc.java_exception)}")
    elif exc and hasattr(exc, "printStackTrace"):
        logger.error(f"{err_msg}\n{str(exc.printStackTrace)}")
    else:
        try:
            logger.error(err_msg, exc)
        except Exception:
            logger.error(f"{err_msg}\n{str(exc)}")


def configure_s3_credentials(
    conf: SparkConf,
    access_key: str = None,
    secret_key: str = None,
    profile: str = None,
    temporary_creds: bool = False,
):
    """Set Spark config values allowing authentication to S3 for bucket data

    See Also:
        Details on authenticating to AWS for s3a access:
          - https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3
          -                                ^---- change docs to version of hadoop being used

    Args:
        conf: Spark configuration object
        access_key: AWS Access Key ID
        secret_key: AWS Secret Access Key
        profile: AWS profile, from which to derive access key and secret key
        temporary_creds: When set to True, use ``org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider``
          - This provider issues short-lived credentials that are routinely refreshed on the client system. Typically
            the client uses an AWS_PROFILE, under which the credentials are refreshed. When authenticating with these
            credentials, the access_key, secret_key, and token must be provided. Additionally the endpoint to a Security
            Token Service that can validate that the given temporary credentials were in fact issued must be configured.
    """
    if access_key and secret_key and not profile and not temporary_creds:
        # Short-circuit the need for boto3 if the caller gave the creds directly as access/secret keys
        conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
        return

    # Use boto3 Session to derive creds
    aws_creds = get_aws_credentials(access_key, secret_key, profile)
    conf.set("spark.hadoop.fs.s3a.access.key", aws_creds.access_key)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_creds.secret_key)
    if temporary_creds:
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )
        conf.set("spark.hadoop.fs.s3a.session.token", aws_creds.token)
        conf.set("spark.hadoop.fs.s3a.assumed.role.sts.endpoint", CONFIG.AWS_STS_ENDPOINT)
        conf.set("spark.hadoop.fs.s3a.assumed.role.sts.endpoint.region", CONFIG.AWS_REGION)


def log_spark_config(spark: SparkSession, config_key_contains=""):
    """Log at log4j INFO the values of the SparkConf object in the current SparkSession"""
    [
        logger.info(f"{item[0]}={item[1]}")
        for item in spark.sparkContext.getConf().getAll()
        if config_key_contains in item[0]
    ]


def log_hadoop_config(spark: SparkSession, config_key_contains=""):
    """Print out to the log the current config values for hadoop. Limit to only those whose key contains the string
    provided to narrow in on a particular subset of config values.
    """
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    [
        logger.info(f"{k}={v}")
        for (k, v) in {str(_).split("=")[0]: str(_).split("=")[1] for _ in conf.iterator()}.items()
        if config_key_contains in k
    ]


def load_dict_to_delta_table(spark, s3_data_bucket, table_schema, table_name, data, overwrite=False):
    """Create a table in Delta and populate it with the contents of a provided dicationary (data). This should
    primarily be used for unit testing.

    Args:
        spark - SparkSession used to run Spark commands
        s3_data_bucket - Bucket where Delta table will be created
        table_schema - Override the default schema from the TABLE_SPEC for the Delta table being created
        table_name - Name of table to create in Delta. This parameter will also be used to lookup a TABLE_SPEC
        data - List of dictionaries containing data to load into the table. Should at least contain the table's
            required fields. Other fields are optional
        overwrite - If this parameter is set to true, the table will be overwritten with each call. When set
            to False, the table will be inserted into instead.
    """
    table_to_col_names_dict = {}
    table_to_col_names_dict["transaction_fabs"] = TRANSACTION_FABS_COLUMNS
    table_to_col_names_dict["transaction_fpds"] = TRANSACTION_FPDS_COLUMNS
    table_to_col_names_dict["transaction_normalized"] = list(TRANSACTION_NORMALIZED_COLUMNS)
    table_to_col_names_dict["awards"] = list(AWARDS_COLUMNS)
    table_to_col_names_dict["financial_accounts_by_awards"] = list(FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS)
    table_to_col_names_dict["detached_award_procurement"] = list(DETACHED_AWARD_PROCUREMENT_DELTA_COLUMNS)
    table_to_col_names_dict["published_fabs"] = list(PUBLISHED_FABS_COLUMNS)

    table_to_col_info_dict = {}
    for tbl_name, col_info in zip(
        ("transaction_fabs", "transaction_fpds"), (TRANSACTION_FABS_COLUMN_INFO, TRANSACTION_FPDS_COLUMN_INFO)
    ):
        table_to_col_info_dict[tbl_name] = {}
        for col in col_info:
            table_to_col_info_dict[tbl_name][col.dest_name] = col

    # Make sure the table has been created first
    call_command(
        "create_delta_table",
        "--destination-table",
        table_name,
        "--alt-db",
        table_schema,
        "--spark-s3-bucket",
        s3_data_bucket,
    )

    if data:
        insert_sql = f"INSERT {'OVERWRITE' if overwrite else 'INTO'} {table_schema}.{table_name} VALUES\n"
        row_strs = []
        for row in data:
            value_strs = []
            for col_name in table_to_col_names_dict[table_name]:
                value = row.get(col_name)
                if isinstance(value, (str, bytes)):
                    # Quote strings for insertion into DB
                    value_strs.append(f"'{value}'")
                elif isinstance(value, (date, datetime)):
                    # Convert to string and quote
                    value_strs.append(f"""'{value.isoformat()}'""")
                elif isinstance(value, bool):
                    value_strs.append(str(value).upper())
                elif isinstance(value, (Sequence, Set)):
                    # Assume "sequences" must be "sequences" of strings, so quote each item in the "sequence"
                    value = [f"'{item}'" for item in value]
                    value_strs.append(f"ARRAY({', '.join(value)})")
                elif value is None:
                    col_info = table_to_col_info_dict.get(table_name)
                    if (
                        col_info
                        and col_info[col_name].delta_type.upper() == "BOOLEAN"
                        and not col_info[col_name].handling == "leave_null"
                    ):
                        # Convert None/NULL to false for boolean columns unless specified to leave the null
                        value_strs.append("FALSE")
                    else:
                        value_strs.append("NULL")
                else:
                    value_strs.append(str(value))

            row_strs.append(f"    ({', '.join(value_strs)})")

        sql = "".join([insert_sql, ",\n".join(row_strs), ";"])
        spark.sql(sql)


def clean_postgres_sql_for_spark_sql(
    postgres_sql_str: str, global_temp_view_proxies: List[str] = None, identifier_replacements: Dict[str, str] = None
):
    """Convert some of the known-to-be-problematic PostgreSQL syntax, which is not compliant with Spark SQL,
    to an acceptable and compliant Spark SQL alternative.

    CAUTION: There is no guarantee that this will convert everything, AND some liberties are taken here to convert
    incompatible data types like JSON to string. USE AT YOUR OWN RISK and make sure its transformations fit your use
    case!

    Arguments:
        postgres_sql_str (str): the SQL string to clean
        global_temp_view_proxies (List[str]): list of table names that should be prepended with the global_temp
            schema if they exist in the hive metastore as a GLOBAL TEMPORARY VIEW
        identifier_replacements (Dict[str, str]): given names (dict keys), that if found (preceded by whitespace)
            will be replaced with the provided alternate name (dict value)

    """
    # Spark SQL does not like double-quoted identifiers
    spark_sql = postgres_sql_str.replace('"', "")
    spark_sql = re.sub(rf"CREATE VIEW", rf"CREATE OR REPLACE TEMP VIEW", spark_sql, flags=re.IGNORECASE | re.MULTILINE)

    # Treat these type casts as string in Spark SQL
    # NOTE: If replacing a ::JSON cast, be sure that the string data coming from delta is treated as needed (e.g. as
    # JSON or converted to JSON or a dict) on the receiving side, and not just left as a string
    spark_sql = re.sub(rf"::text|::json", rf"::string", spark_sql, flags=re.IGNORECASE | re.MULTILINE)

    if global_temp_view_proxies:
        for vw in global_temp_view_proxies:
            spark_sql = re.sub(
                rf"FROM\s+{vw}", rf"FROM global_temp.{vw}", spark_sql, flags=re.IGNORECASE | re.MULTILINE
            )
            spark_sql = re.sub(
                rf"JOIN\s+{vw}", rf"JOIN global_temp.{vw}", spark_sql, flags=re.IGNORECASE | re.MULTILINE
            )

    if identifier_replacements:
        for old, new in identifier_replacements.items():
            matches = re.finditer(rf"(\s+|^|\(){old}(\s+|$|\()", spark_sql, flags=re.IGNORECASE | re.MULTILINE)
            for match in matches:
                spark_sql = re.sub(
                    re.escape(rf"{match[0]}"),
                    rf"{match[1]}{new}{match[2]}",
                    spark_sql,
                    flags=re.IGNORECASE | re.MULTILINE,
                )
    return spark_sql
