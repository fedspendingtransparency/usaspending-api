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

from usaspending_api.config.utils import ENV_SPECIFIC_OVERRIDE, eval_default_factory
from pydantic import (
    AnyHttpUrl,
    BaseSettings,
    PostgresDsn,
    SecretStr,
    validator,
)
from pydantic.fields import ModelField

_PROJECT_NAME = "usaspending-api"
_PROJECT_ROOT_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.parent.resolve()
_SRC_ROOT_DIR: pathlib.Path = _PROJECT_ROOT_DIR / _PROJECT_NAME.replace("-", "_")


class DefaultConfig(BaseSettings):
    """Top-level config that defines all configuration variables, and their default, overridable values

    Attributes:
        POSTGRES_URL: (optional) Full URL to Postgres DB  that can be used to override the URL-by-parts
        POSTGRES_DB: The name of the Postgres DB that contains USAspending data
        POSTGRES_USER: Authorized user used to connect to the USAspending DB
        POSTGRES_PASSWORD: Password for the user used to connect to the USAspending DB
        POSTGRES_HOST: Host on which to to connect to the USAspending DB
        POSTGRES_PORT: Port on which to connect to the USAspending DB
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
    POSTGRES_URL: str = None
    POSTGRES_DB: str = "data_store_api"
    POSTGRES_USER: str = ENV_SPECIFIC_OVERRIDE
    POSTGRES_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE
    POSTGRES_HOST: str = ENV_SPECIFIC_OVERRIDE
    POSTGRES_PORT: str = ENV_SPECIFIC_OVERRIDE
    POSTGRES_DSN: PostgresDsn = None  # FACTORY_PROVIDED_VALUE. See below validator-factory

    @validator("POSTGRES_DSN")
    def _POSTGRES_DSN_factory(cls, v, values, field: ModelField):
        def factory_func() -> PostgresDsn:
            """A factory that assembles the full DSN URL to the Postgres DB.

            If ``POSTGRES_URL`` is provided e.g. as an env var, it will be used. Otherwise this URL is assembled from
            the other ``POSTGRES_*`` config vars
            """
            path = None
            if values["POSTGRES_DB"]:
                path = "/" + values["POSTGRES_DB"]
            return PostgresDsn(
                url=values["POSTGRES_URL"],
                scheme="postgres",
                user=values["POSTGRES_USER"],
                password=values["POSTGRES_PASSWORD"].get_secret_value(),
                host=values["POSTGRES_HOST"],
                port=values["POSTGRES_PORT"],
                path=path,
            )

        return eval_default_factory(cls, v, values, field, factory_func)

    # ==== [Elasticsearch] ====
    # Where to connect to elasticsearch.
    ES_URL: str = None
    ES_SCHEME: str = "https"
    ES_HOST: str = ENV_SPECIFIC_OVERRIDE
    ES_PORT: str = ""

    ELASTICSEARCH_HOST: AnyHttpUrl = None  # FACTORY_PROVIDED_VALUE. See below validator-factory

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
    DELTA_LAKE_S3_PATH: str = "data/delta"  # path within SPARK_S3_BUCKET where output data will accumulate
    AWS_S3_ENDPOINT: str = "s3.us-gov-west-1.amazonaws.com"
    AWS_STS_ENDPOINT: str = "sts.us-gov-west-1.amazonaws.com"

    class Config:
        pass
        # supporting use of a user-provided (ang git-ignored) .env file for overrides
        env_file = str(_PROJECT_ROOT_DIR / ".env")
        env_file_encoding = "utf-8"
