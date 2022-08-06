########################################################################################################################
# DEFAULT runtime configuration
# - Config variables declared here should cover ALL expected config variables for a "production" run
# - Non-Prod runtime env config may include additional config not defined here, to accommodate their different needs
# - Set config var to non-placeholder values here that are acceptable defaults for any runtime environment
# - Set config var to ENV_SPECIFIC_OVERRIDE where there is expected to be a replacement config value
#   in every runtime environment config
# - Values can be overridden if the same config variable is defined in a runtime-env-specific <env>.py file
# - Values will be overridden by ENVIRONMENT variables declared in a user's .env file if present at ../.env
# - See https://pydantic-docs.helpmanual.io/usage/settings/#field-value-priority for precedence of overrides
########################################################################################################################
import pathlib
from typing import ClassVar

from usaspending_api.config.utils import (
    ENV_SPECIFIC_OVERRIDE,
    eval_default_factory,
    eval_default_factory_from_root_validator,
    CONFIG_VAR_PLACEHOLDERS,
    parse_pg_uri,
    unveil,
)
from pydantic import (
    AnyHttpUrl,
    BaseSettings,
    PostgresDsn,
    SecretStr,
    validator,
    root_validator,
)
from pydantic.fields import ModelField

_PROJECT_NAME = "usaspending-api"
# WARNING: This is relative to THIS file's location. If it is moved/refactored, this needs to be confirmed to point
# to the project root dir (i.e. usaspending-api/)
_PROJECT_ROOT_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
_SRC_ROOT_DIR: pathlib.Path = _PROJECT_ROOT_DIR / _PROJECT_NAME.replace("-", "_")


class DefaultConfig(BaseSettings):
    """Top-level config that defines all configuration variables, and their default, overridable values

    Attributes:
        DATABASE_URL: (optional) Full URL to Postgres DB  that can be used to override the URL-by-parts
        USASPENDING_DB_NAME: The name of the Postgres DB that contains USAspending data
        USASPENDING_DB_USER: Authorized user used to connect to the USAspending DB
        USASPENDING_DB_PASSWORD: Password for the user used to connect to the USAspending DB
        USASPENDING_DB_HOST: Host on which to to connect to the USAspending DB
        USASPENDING_DB_PORT: Port on which to connect to the USAspending DB
        DATA_BROKER_DATABASE_URL: (optional) Full URL to Broker DB that can be used to override the URL-by-parts
        BROKER_DB_NAME: The name of the Postgres DB that contains Broker data
        BROKER_DB_USER: Authorized user used to connect to the Broker DB
        BROKER_DB_PASSWORD: Password for the user used to connect to the Broker DB
        BROKER_DB_HOST: Host on which to to connect to the Broker DB
        BROKER_DB_PORT: Port on which to connect to the Broker DB
        ES_URL: (optional) Full URL to Elasticsearch cluster that can be used to override the URL-by-parts
        ES_SCHEME: "http" or "https". Defaults to "https:
        ES_HOST: Host on which to connect to the USAspending Elasticsearch cluster
        ES_PORT: Port on which to connect to the USAspending Elasticsearch cluster, if different than 80 or 443
                 Defaults to empty string ("")
    """

    def __new__(cls, *args, **kwargs):
        if cls is DefaultConfig:
            raise NotImplementedError(
                "DefaultConfig is just the base config and should not be instantiated. "
                "Instantiate a subclass of this for a specific runtime environment."
            )
        return super().__new__(cls)

    # ==== [Global] ====
    ENV_CODE: ClassVar[str] = ENV_SPECIFIC_OVERRIDE
    COMPONENT_NAME: str = "USAspending API"
    PROJECT_LOG_DIR: str = str(_SRC_ROOT_DIR / "logs")

    # ==== [Postgres] ====
    DATABASE_URL: str = None  # FACTORY_PROVIDED_VALUE. See its root validator-factory below
    USASPENDING_DB_NAME: str = "data_store_api"
    USASPENDING_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PORT: str = ENV_SPECIFIC_OVERRIDE

    DATA_BROKER_DATABASE_URL: str = None  # FACTORY_PROVIDED_VALUE. See its root validator-factory below
    BROKER_DB_NAME: str = "data_broker"
    BROKER_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PORT: str = ENV_SPECIFIC_OVERRIDE

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    def _validate_database_url(cls, values, db_url_conf_name, db_conf_prefix, required=True):
        """ Helper function to validate both DATABASE_URLs and their parts """

        # First determine if DB URL config was provided.
        is_database_url_provided = values[db_url_conf_name] and values[db_url_conf_name] not in CONFIG_VAR_PLACEHOLDERS

        # If the DB URL config was was provided
        # - it should take precedence
        # - its values will be used to backfill any missing POSTGRES DSN parts stored as separate config vars
        if is_database_url_provided:
            url_parts, username, password = parse_pg_uri(values[db_url_conf_name])
            backfill_configs = {
                f"{db_conf_prefix}_HOST": lambda: url_parts.hostname,
                f"{db_conf_prefix}_PORT": lambda: str(url_parts.port),
                f"{db_conf_prefix}_NAME": lambda: url_parts.path.lstrip("/"),
                f"{db_conf_prefix}_USER": lambda: username,
                f"{db_conf_prefix}_PASSWORD": lambda: SecretStr(password),
            }
            # Backfill only POSTGRES DSN CONFIG vars that are missing their value
            for config_name, transformation in backfill_configs.items():
                value = values.get(config_name, None)
                if config_name == f"{db_conf_prefix}_PASSWORD":
                    value = unveil(value)
                if value in [None] + CONFIG_VAR_PLACEHOLDERS:
                    values = eval_default_factory_from_root_validator(cls, values, config_name, transformation)

        # If the DB URL config is not provided, try to build-it-up from provided parts, then backfill it
        if not is_database_url_provided:
            # First validate that we have enough parts to provide it
            required_pg_dsn_parts = [
                f"{db_conf_prefix}_HOST",
                f"{db_conf_prefix}_PORT",
                f"{db_conf_prefix}_NAME",
                f"{db_conf_prefix}_USER",
                f"{db_conf_prefix}_PASSWORD",
            ]
            if not (
                all(pg_dsn_part in values for pg_dsn_part in required_pg_dsn_parts)
                and all(values[pg_dsn_part] is not None for pg_dsn_part in required_pg_dsn_parts)
            ):
                if required:
                    raise ValueError(
                        f"PostgreSQL {db_url_conf_name} was not provided and could not be built because one or more of"
                        " these required parts were missing. Please check that the parts are completely configured, or"
                        f" provide a {db_url_conf_name} instead: {required_pg_dsn_parts}"
                    )
                else:
                    print(
                        f"PostgreSQL {db_url_conf_name} was not provided and could not be built because one or more of"
                        " these required parts were missing. Leaving blank for now. Please check that the parts are"
                        f" completely configured, or provide a {db_url_conf_name} to use it:"
                        f" {required_pg_dsn_parts}"
                    )
            else:
                pg_dsn = PostgresDsn(
                    url=None,
                    scheme="postgres",
                    user=values[f"{db_conf_prefix}_USER"],
                    password=values[f"{db_conf_prefix}_PASSWORD"].get_secret_value(),
                    host=values[f"{db_conf_prefix}_HOST"],
                    port=values[f"{db_conf_prefix}_PORT"],
                    path="/" + values[f"{db_conf_prefix}_NAME"] if values[f"{db_conf_prefix}_NAME"] else None,
                )
                values = eval_default_factory_from_root_validator(cls, values, db_url_conf_name, lambda: str(pg_dsn))

        # if the DB URL is now available, check for consistency with the db config values
        if values.get(db_url_conf_name, None):
            # Now validate the provided and/or built values are consistent between DB URL and db config parts
            pg_url_config_errors = {}
            pg_url_parts, pg_username, pg_password = parse_pg_uri(values[db_url_conf_name])

            # Validate host
            if pg_url_parts.hostname != values[f"{db_conf_prefix}_HOST"]:
                pg_url_config_errors[f"{db_conf_prefix}_HOST"] = (
                    values[f"{db_conf_prefix}_HOST"],
                    pg_url_parts.hostname,
                )

            # Validate port
            if (
                (pg_url_parts.port is not None and str(pg_url_parts.port) != values[f"{db_conf_prefix}_PORT"])
                or pg_url_parts.port is None
                and values[f"{db_conf_prefix}_PORT"] is not None
            ):
                pg_url_config_errors[f"{db_conf_prefix}_PORT"] = (values[f"{db_conf_prefix}_PORT"], pg_url_parts.port)

            # Validate DB name (path)
            if (
                (pg_url_parts.path is not None and pg_url_parts.path.lstrip("/") != values[f"{db_conf_prefix}_NAME"])
                or pg_url_parts.path is None
                and values[f"{db_conf_prefix}_NAME"] is not None
            ):
                pg_url_config_errors[f"{db_conf_prefix}_NAME"] = (
                    values[f"{db_conf_prefix}_NAME"],
                    pg_url_parts.path.lstrip("/")
                    if pg_url_parts.path is not None and pg_url_parts.path != "/"
                    else None,
                )

            # Validate username
            if pg_username != values[f"{db_conf_prefix}_USER"]:
                pg_url_config_errors[f"{db_conf_prefix}_USER"] = (values[f"{db_conf_prefix}_USER"], pg_username)

            # Validate password
            if pg_password != unveil(values[f"{db_conf_prefix}_PASSWORD"]):
                # NOTE: Keeping password text obfuscated in the error output
                pg_url_config_errors[f"{db_conf_prefix}_PASSWORD"] = (
                    values[f"{db_conf_prefix}_PASSWORD"],
                    "*" * len(pg_password) if pg_password is not None else None,
                )

            if len(pg_url_config_errors) > 0:
                err_msg = (
                    f"The {db_url_conf_name} config var value was provided along with one or more {db_conf_prefix}_*"
                    "config var values, however they were not consistent. Differing configuration sources that should "
                    "match will cause confusion. Either provide all values consistently, or only provide a complete "
                    f"{db_url_conf_name} and leave the others as None or unset, or leave "
                    f"the {db_url_conf_name} None or unset and provide only the required POSTGRES DSN parts. "
                    "Parts not matching:\n"
                )
                for k, v in pg_url_config_errors.items():
                    err_msg += f"\tPart: {k}, Part Value Provided: {v[0]}, Value found in {db_url_conf_name}: {v[1]}\n"
                raise ValueError(err_msg)

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    @root_validator
    def _DATABASE_URL_and_parts_factory(cls, values):
        """A root validator to backfill DATABASE_URL and USASPENDING_DB_* part config vars and validate that they are
        all consistent.

        - Serves as a factory function to fill out all places where we track the database URL as both one complete
        connection string and as individual parts.
        - ALSO validates that the parts and whole string are consistent. A ``ValueError`` is thrown if found to
        be inconsistent, which will in turn raise a ``pydantic.ValidationError`` at configuration time.
        """
        # noinspection PyArgumentList
        cls._validate_database_url(
            cls=cls, values=values, db_url_conf_name="DATABASE_URL", db_conf_prefix="USASPENDING_DB", required=True
        )
        return values

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    @root_validator
    def _DATA_BROKER_DATABASE_URL_and_parts_factory(cls, values):
        """A root validator to backfill DATA_BROKER_DATABASE_URL and BROKER_DB_* part config vars and validate
        that they are all consistent.

        - Serves as a factory function to fill out all places where we track the database URL as both one complete
        connection string and as individual parts.
        - ALSO validates that the parts and whole string are consistent. A ``ValueError`` is thrown if found to
        be inconsistent, which will in turn raise a ``pydantic.ValidationError`` at configuration time.
        """
        # noinspection PyArgumentList
        cls._validate_database_url(
            cls=cls,
            values=values,
            db_url_conf_name="DATA_BROKER_DATABASE_URL",
            db_conf_prefix="BROKER_DB",
            required=False,
        )
        return values

    # ==== [Elasticsearch] ====
    # Where to connect to elasticsearch.
    ES_URL: str = None
    ES_SCHEME: str = "https"
    ES_HOST: str = ENV_SPECIFIC_OVERRIDE
    ES_PORT: str = ""

    ELASTICSEARCH_HOST: AnyHttpUrl = None  # FACTORY_PROVIDED_VALUE. See below validator-factory

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    @validator("ELASTICSEARCH_HOST")
    def _ELASTICSEARCH_HOST_factory(cls, v, values, field: ModelField):
        def factory_func() -> AnyHttpUrl:
            """A factory that assembles the full URL to a single Elasticsearch host, to which ES cluster
            connections are made.

                If ``ES_URL`` is provided as e.g. an env var, it will be used. Otherwise this URL is assembled from the
                other ``ES_*`` config vars
            """
            return AnyHttpUrl(
                url=values["ES_URL"],
                scheme=values["ES_SCHEME"],
                host=values["ES_HOST"],
                port=values["ES_PORT"],
            )

        return eval_default_factory(cls, v, values, field, factory_func)

    # ==== [Spark] ====
    # SPARK_SCHEDULER_MODE = "FAIR"  # if used with weighted pools, could allow round-robin tasking of simultaneous jobs
    # TODO: have to deal with this if really wanting balanced (FAIR) task execution
    # WARN FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO
    # order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a
    # file that contains the configuration.
    # 01:58:26 INFO FairSchedulableBuilder: Created default pool: default, schedulingMode: FIFO, minShare: 0, weight: 1
    SPARK_SCHEDULER_MODE: str = "FIFO"  # the default Spark scheduler mode

    # Our Postgres takes 6 digits of precision (to the microsecond), so show that here.
    # The single [x] will conform to the ISO 8601 SQL standard (e.g. +00 for UTC, -04 for NY, +0530 for IST)
    # More info: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    SPARK_CSV_TIMEZONE_FORMAT: str = "yyyy-MM-dd HH:mm:ss[.SSSSSS][x]"

    # Ideal partition size based on a single Executor's task execution, and what it can handle in memory: ~128MB
    # Given the density of this data, 128 MB bulk-index request to Elasticsearch is about 75,000 docs
    # However ES does not appear to be able too handle that when several Executors make that request in parallel
    # Good tips from ES: https:#www.elastic.co/guide/en/elasticsearch/reference/6.2/tune-for-indexing-speed.html
    # Reducing to 10,000 DB rows per bulk indexing operation
    SPARK_PARTITION_ROWS: int = 10000
    SPARK_MAX_PARTITIONS: int = 100000

    # Spark is connecting JDBC to Elasticsearch here and this config calibrates the throughput from one to the other,
    # and have to accommodate limitations on either side of the pipe.
    # `partition_rows` above is how many data items come out of the JDBC side, and are handled by one Executor
    # process's task on a Spark cluster node.
    # But ES batch config below throttle how that Executor breaks the received data into batches going out to the
    # ES cluster.
    # So, given rows of 10,000, and max batch entries of 4000, the executor will still process all 10,000 rows before
    # quitting its task, it may just make 2 or 3 HTTP requests to ES to do it.

    # Assuming >=4GB RAM per vCPU of the data nodes in the ES cluster, use below config for calibrating the max
    # batch size of JSON docs for each bulk indexing HTTP request to the ES cluster
    # Indexing should get distributed among the cluster
    # Mileage may vary - try out different batch sizes
    # It may be better to see finer-grain metrics by using higher request rates with shorter index-durations,
    # rather than a busier indexing process (i.e. err on the smaller batch sizes to get feedback)
    # Aiming for a batch that yields each ES cluster data-node handling max 0.3-1MB per vCPU per batch request
    # Ex: 3-data-node cluster of i3.large.elasticsearch = 2 vCPU * 3 nodes = 6 vCPU: .75MB*6 ~= 4.5MB batches
    # Ex: 5-data-node cluster of i3.xlarge.elasticsearch = 4 vCPU * 5 nodes = 20 vCPU: .75MB*20 ~= 15MB batches
    ES_MAX_BATCH_BYTES: int = 10 * 1024 * 1024
    # Aiming for a batch that yields each ES cluster data-node handling max 100-400 doc entries per vCPU per request
    # Ex: 3-data-node cluster of i3.large.elasticsearch = 2 vCPU * 3 nodes = 6 vCPU: 300*6 = 1800 doc batches
    # Ex: 5-data-node cluster of i3.xlarge.elasticsearch = 4 vCPU * 5 nodes = 20 vCPU: 300*20 = 6000 doc batches
    ES_BATCH_ENTRIES: int = 4000

    # ==== [AWS] ====
    USE_AWS: bool = True
    AWS_REGION: str = "us-gov-west-1"
    AWS_ACCESS_KEY: SecretStr = ENV_SPECIFIC_OVERRIDE
    AWS_SECRET_KEY: SecretStr = ENV_SPECIFIC_OVERRIDE
    # Setting AWS_PROFILE to None so boto3 doesn't try to pick up the placeholder string as an actual profile to find
    AWS_PROFILE: str = None  # USER_SPECIFIC_OVERRIDE
    SPARK_S3_BUCKET: str = ENV_SPECIFIC_OVERRIDE
    DELTA_LAKE_S3_PATH: str = "data/delta"  # path within SPARK_S3_BUCKET where Delta output data will accumulate
    SPARK_CSV_S3_PATH: str = "data/csv"  # path within SPARK_S3_BUCKET where CSV output data will accumulate
    AWS_S3_ENDPOINT: str = "s3.us-gov-west-1.amazonaws.com"
    AWS_STS_ENDPOINT: str = "sts.us-gov-west-1.amazonaws.com"

    class Config:
        pass
        # supporting use of a user-provided (ang git-ignored) .env file for overrides
        env_file = str(_PROJECT_ROOT_DIR / ".env")
        env_file_encoding = "utf-8"
