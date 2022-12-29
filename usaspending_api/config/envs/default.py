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
    unveil, parse_http_url,
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
        ES_URL: (optional, but preferred) Full URL to Elasticsearch cluster that can be used to override the
                URL-by-parts
        ES_SCHEME: "http" or "https". Defaults to "https:
        ES_HOST: Host on which to connect to the USAspending Elasticsearch cluster
        ES_PORT: Port on which to connect to the USAspending Elasticsearch cluster, if different than 80 or 443
                 Defaults to empty string ("")
        ES_NAME: Defaults to None and not set. Here for compatibility with URL-based configs
        ES_USER: Username to be used if basic auth is required for ES connections
        ES_PASSWORD: Password to be used if basic auth is required for ES connections
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
    USASPENDING_DB_SCHEME: str = "postgres"
    USASPENDING_DB_NAME: str = "data_store_api"
    USASPENDING_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PORT: str = ENV_SPECIFIC_OVERRIDE

    DATA_BROKER_DATABASE_URL: str = None  # FACTORY_PROVIDED_VALUE. See its root validator-factory below
    BROKER_DB_SCHEME: str = "postgres"
    BROKER_DB_NAME: str = "data_broker"
    BROKER_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PORT: str = ENV_SPECIFIC_OVERRIDE

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    def _validate_database_url(cls, values, url_conf_name, resource_conf_prefix, required=True):
        """ Helper function to validate both DATABASE_URLs and their parts """

        # First determine if full URL config was provided.
        is_full_url_provided = cls._check_for_full_url_config(url_conf_name, values)

        # If the full URL config was provided
        # - it should take precedence
        # - its values will be used to backfill any missing URL parts stored as separate config vars
        if is_full_url_provided:
            values = cls._backfill_url_parts_config(url_conf_name, resource_conf_prefix, values)

        # If the full URL config is not provided, try to build-it-up from provided parts, then set the full URL
        if not is_full_url_provided:
            # First validate that we have enough parts to provide it
            enough_parts = cls._check_required_url_parts(
                error_if_missing=required,
                url_conf_name=url_conf_name,
                resource_conf_prefix=resource_conf_prefix,
                values=values,
            )

            if enough_parts:
                pg_dsn = PostgresDsn(
                    url=None,
                    scheme=values[f"{resource_conf_prefix}_SCHEME"],
                    user=values[f"{resource_conf_prefix}_USER"],
                    password=values[f"{resource_conf_prefix}_PASSWORD"].get_secret_value(),
                    host=values[f"{resource_conf_prefix}_HOST"],
                    port=values[f"{resource_conf_prefix}_PORT"],
                    path="/" + values[f"{resource_conf_prefix}_NAME"] if values[
                        f"{resource_conf_prefix}_NAME"] else None,
                )
                values = eval_default_factory_from_root_validator(cls, values, url_conf_name, lambda: str(pg_dsn))

        # if the fully configured URL is now available, check for consistency with the URL-part config values
        if values.get(url_conf_name, None):
            cls._validate_url_and_parts(url_conf_name, resource_conf_prefix, values)

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
    ES_URL: str = None  # FACTORY_PROVIDED_VALUE. See below validator-factory
    ES_SCHEME: str = "https"
    ES_HOST: str = ENV_SPECIFIC_OVERRIDE
    ES_PORT: str = ""
    ES_USER: str = None
    ES_PASSWORD: SecretStr = None
    ES_NAME: str = None

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    @root_validator
    def _ES_URL_and_parts_factory(cls, values):
        """A root validator to backfill ES_URL and ES_* part config vars and validate that they are
                all consistent.

                - Serves as a factory function to fill out all places where we track the ES URL as both one
                complete connection string and as individual parts.
                - ALSO validates that the parts and whole string are consistent. A ``ValueError`` is thrown if found to
                be inconsistent, which will in turn raise a ``pydantic.ValidationError`` at configuration time.
                """
        # noinspection PyArgumentList
        cls._validate_http_url(
            cls=cls, values=values, url_conf_name="ES_URL", resource_conf_prefix="ES", required=False
        )
        return values

    # noinspection PyMethodParameters
    # Pydantic returns a classmethod for its validators, so the cls param is correct
    def _validate_http_url(cls, values, url_conf_name, resource_conf_prefix, required=True):
        """ Helper function to validate complete URLs and their individual parts when either/both are provided """

        # First determine if full URL config was provided.
        is_full_url_provided = cls._check_for_full_url_config(url_conf_name, values)

        # If the full URL config was provided
        # - it should take precedence
        # - its values will be used to backfill any missing URL parts stored as separate config vars
        if is_full_url_provided:
            values = cls._backfill_url_parts_config(url_conf_name, resource_conf_prefix, values)

        # If the full URL config is not provided, try to build-it-up from provided parts, then set the full URL
        if not is_full_url_provided:
            # First validate that we have enough parts to provide it
            enough_parts = cls._check_required_url_parts(
                error_if_missing=required,
                url_conf_name=url_conf_name,
                resource_conf_prefix=resource_conf_prefix,
                values=values,
            )

            if enough_parts:
                http_url = AnyHttpUrl(
                    url=None,
                    scheme=values[f"{resource_conf_prefix}_SCHEME"],
                    user=values[f"{resource_conf_prefix}_USER"],
                    password=values[f"{resource_conf_prefix}_PASSWORD"].get_secret_value(),
                    host=values[f"{resource_conf_prefix}_HOST"],
                    port=values[f"{resource_conf_prefix}_PORT"],
                    path="/" + values[f"{resource_conf_prefix}_NAME"] if values[f"{resource_conf_prefix}_NAME"] else None,
                )
                values = eval_default_factory_from_root_validator(cls, values, url_conf_name, lambda: str(http_url))

        # if the fully configured URL is now available, check for consistency with the URL-part config values
        if values.get(url_conf_name, None):
            cls._validate_url_and_parts(url_conf_name, resource_conf_prefix, values)

    def _check_required_url_parts(cls, error_if_missing, url_conf_name, resource_conf_prefix, values):
        enough_parts = False
        required_url_parts = [
            f"{resource_conf_prefix}_SCHEME",
            f"{resource_conf_prefix}_HOST",
            f"{resource_conf_prefix}_PORT",
            f"{resource_conf_prefix}_NAME",
            f"{resource_conf_prefix}_USER",
            f"{resource_conf_prefix}_PASSWORD",
        ]
        if not (
                all(url_part in values for url_part in required_url_parts)
                and all(values[url_part] is not None for url_part in required_url_parts)
        ):
            if error_if_missing:
                raise ValueError(
                    f"Config URL {url_conf_name} was not provided and could not be built because one or more of"
                    " these required parts were missing. Please check that the parts are completely configured, or"
                    f" provide the complete URL for {url_conf_name} instead: {required_url_parts}"
                )
            else:
                print(
                    f"Config URL {url_conf_name} was not provided and could not be built because one or more of"
                    " these required parts were missing. Leaving blank for now. Please check that the parts are"
                    f" completely configured, or provide a complete URL for {url_conf_name} to use it:"
                    f" {required_url_parts}"
                )
        else:
            enough_parts = True
        return enough_parts

    def _backfill_url_parts_config(cls, url_conf_name, resource_conf_prefix, values):
        url_parts, username, password = parse_http_url(values[url_conf_name])
        backfill_configs = {
            f"{resource_conf_prefix}_SCHEME": lambda: url_parts.scheme,
            f"{resource_conf_prefix}_HOST": lambda: url_parts.hostname,
            f"{resource_conf_prefix}_PORT": lambda: str(url_parts.port),
            f"{resource_conf_prefix}_NAME": lambda: url_parts.path.lstrip("/"),
            f"{resource_conf_prefix}_USER": lambda: username,
            f"{resource_conf_prefix}_PASSWORD": lambda: SecretStr(password),
        }
        # Backfill only URL CONFIG vars that are missing their value
        for config_name, transformation in backfill_configs.items():
            value = values.get(config_name, None)
            if config_name == f"{resource_conf_prefix}_PASSWORD":
                value = unveil(value)
            if value in [None] + CONFIG_VAR_PLACEHOLDERS:
                values = eval_default_factory_from_root_validator(cls, values, config_name, transformation)
        return values

    def _check_for_full_url_config(self, url_conf_name, values):
        return values[url_conf_name] and values[url_conf_name] not in CONFIG_VAR_PLACEHOLDERS

    def _validate_url_and_parts(self, url_conf_name, resource_conf_prefix, values):
        # Now validate the provided and/or built values are consistent between complete URL and URL config parts
        url_config_errors = {}
        url_parts, url_username, url_password = parse_http_url(values[url_conf_name])
        # Validate host
        if url_parts.hostname != values[f"{resource_conf_prefix}_HOST"]:
            url_config_errors[f"{resource_conf_prefix}_HOST"] = (
                values[f"{resource_conf_prefix}_HOST"],
                url_parts.hostname,
            )
        # Validate port
        if (
                (url_parts.port is not None and str(url_parts.port) != values[f"{resource_conf_prefix}_PORT"])
                or url_parts.port is None
                and values[f"{resource_conf_prefix}_PORT"] is not None
        ):
            url_config_errors[f"{resource_conf_prefix}_PORT"] = (values[f"{resource_conf_prefix}_PORT"], url_parts.port)
        # Validate resource name (path)
        if (
                (url_parts.path is not None and url_parts.path.lstrip("/") != values[f"{resource_conf_prefix}_NAME"])
                or url_parts.path is None
                and values[f"{resource_conf_prefix}_NAME"] is not None
        ):
            url_config_errors[f"{resource_conf_prefix}_NAME"] = (
                values[f"{resource_conf_prefix}_NAME"],
                url_parts.path.lstrip("/")
                if url_parts.path is not None and url_parts.path != "/"
                else None,
            )
        # Validate username
        if url_username != values[f"{resource_conf_prefix}_USER"]:
            url_config_errors[f"{resource_conf_prefix}_USER"] = (values[f"{resource_conf_prefix}_USER"], url_username)
        # Validate password
        if url_password != unveil(values[f"{resource_conf_prefix}_PASSWORD"]):
            # NOTE: Keeping password text obfuscated in the error output
            url_config_errors[f"{resource_conf_prefix}_PASSWORD"] = (
                values[f"{resource_conf_prefix}_PASSWORD"],
                "*" * len(url_password) if url_password is not None else None,
            )
        if len(url_config_errors) > 0:
            err_msg = (
                f"The URL config var named {url_conf_name} was provided along with one or more "
                f"{resource_conf_prefix}_* URL-part config var values, however they were not consistent. Differing "
                f"configuration sources that should match will cause confusion. Either provide all values "
                f"consistently, or only provide a complete {url_conf_name} and leave the others as None or unset, "
                f"or leave the {url_conf_name} None or unset and provide only the required URL-parts. "
                "Parts not matching:\n"
            )
            for k, v in url_config_errors.items():
                err_msg += f"\tPart: {k}, Part Value Provided: {v[0]}, Value found in {url_conf_name}: {v[1]}\n"
            raise ValueError(err_msg)

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
    # However ES does not appear to be able to handle that when several Executors make that request in parallel
    # Good tips from ES: https:#www.elastic.co/guide/en/elasticsearch/reference/6.2/tune-for-indexing-speed.html
    # Reducing to 10,000 DB rows per bulk indexing operation
    SPARK_PARTITION_ROWS: int = 10000
    SPARK_MAX_PARTITIONS: int = 100000

    # Minimum number of partitions to split/group batches of CSV files into, which need to be written via COPY to PG
    # i.e. maintain at least this many writers, and therefore this many concurrent open connections writing to the DB
    SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS: int = 8

    # Use this multiplier, in conjunction with the above ``SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS``, to derive the
    # number of partitions to split/group batches of CSV files into, which need to be written via COPY to PG
    # This multiplier will be multiplied against the max_parallel_workers value of the target database. It can be a
    # fraction less than 1.0. The final value will be the greater of that or ``SPARK_CSV_WRITE_TO_PG_MIN_PARTITIONS``
    SPARK_CSV_WRITE_TO_PG_PARALLEL_WORKER_MULTIPLIER: float = 0.5

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
