########################################################################################################################
# LOCAL runtime env configuration
# - Inherits all defaults from DefaultConfig in default.py
# - Overrides any default config variables with local runtime env specific values
# - Users may override local config vars values declared here by putting values in a git-ignored .env file alongside
#   docker-compose.yml
# - Set config variables to DefaultConfig.USER_SPECIFIC_OVERRIDE where there is expected to be a
#   user-provided a config value for a variable (e.g. in the ../.env file)
########################################################################################################################
from typing import ClassVar

from pydantic import root_validator
from pydantic.types import SecretStr
from usaspending_api.config.envs.default import DefaultConfig, _PROJECT_ROOT_DIR
from usaspending_api.config.utils import (
    USER_SPECIFIC_OVERRIDE,
    FACTORY_PROVIDED_VALUE,
    eval_default_factory_from_root_validator,
)


class LocalConfig(DefaultConfig):
    """Config for a local runtime environment, which inherits and overrides from DefaultConfig

    See Also:
        Attributes inherited from or overridden from ``DefaultConfig``

    Attributes:
        MINIO_ACCESS_KEY: Access key for accessing S3 object data stored locally via MinIO
        MINIO_SECRET_KEY: Secret key for accessing S3 object data stored locally via MinIO
        MINIO_DATA_DIR: Where docker persists "object data" for S3 objects stored locally
            - Should point to a path where data can be persistend beyond docker restarts,
              outside of the git source repository
    """

    # ==== [Global] ====
    ENV_CODE: ClassVar[str] = "lcl"

    # Common credentials to share across services for convenience / ease on remembering
    _USASPENDING_USER: SecretStr = "usaspending"
    _USASPENDING_PASSWORD: SecretStr = "usaspender"

    # ==== [Postgres USAS] ====
    USASPENDING_DB_USER: str = _USASPENDING_USER
    USASPENDING_DB_PASSWORD: SecretStr = _USASPENDING_PASSWORD

    # Change to host.docker.internal if you are running a local Postgres. Otherwise leave as-is, so
    # Docker will use the Postgres created by Compose.
    USASPENDING_DB_HOST: str = "usaspending-db"
    USASPENDING_DB_PORT: str = "5432"

    # ==== [Postgres Broker] ====
    BROKER_DB_USER: str = "admin"
    BROKER_DB_PASSWORD: SecretStr = "root"

    # Change to host.docker.internal if you are running a local Postgres. Otherwise leave as-is, so
    # Docker will use the Postgres created by Compose.
    BROKER_DB_HOST: str = "dataact-broker-db"
    BROKER_DB_PORT: str = "5432"

    # ==== [Elasticsearch] ====
    # Where to connect to elasticsearch.
    ES_SCHEME: str = "http"
    ES_HOST: str = "localhost"
    ES_PORT: str = "9200"

    # ==== [Spark] ====
    # Sensible defaults to underneath the project root dir. But look in .env for overriding of these
    SPARK_SQL_WAREHOUSE_DIR: str = str(_PROJECT_ROOT_DIR / "spark-warehouse")
    HIVE_METASTORE_DERBY_DB_DIR: str = str(_PROJECT_ROOT_DIR / "spark-warehouse" / "metastore_db")
    SPARK_COVID19_DOWNLOAD_README_FILE_PATH = str(_PROJECT_ROOT_DIR / "data" / "COVID-19_download_readme.txt")

    # ==== [MinIO] ====
    MINIO_HOST: str = "localhost"
    # Changing MinIO ports from defaults. Known to have port conflicts with proxies on developer laptops
    MINIO_PORT: str = "10001"
    MINIO_CONSOLE_PORT: str = "10002"
    MINIO_ACCESS_KEY: SecretStr = _USASPENDING_USER  # likely overridden in .env
    MINIO_SECRET_KEY: SecretStr = _USASPENDING_PASSWORD  # likely overridden in .env
    # Should point to a path where data can be persistend beyond docker restarts, outside of the git source repository
    MINIO_DATA_DIR: str = USER_SPECIFIC_OVERRIDE

    # ==== [AWS] ====
    # In local dev env, default to NOT using AWS.
    # - For S3, MinIO will be used, and the AWS Endpoints defaulted below  will be used by MinIO to connect to "S3"
    #   locally.
    # - If you want to connect to AWS from your local dev env setup, for S3 as the backing object store of data,
    #   set this to True, and change the AWS endpoints/region to that of the targeted AWS account
    # - Then you MUST set your AWS creds (access/secret/token) by way of setting AWS_PROFILE env var (e.g. in your
    #   .env file)
    USE_AWS: bool = False
    AWS_ACCESS_KEY: SecretStr = MINIO_ACCESS_KEY
    AWS_SECRET_KEY: SecretStr = MINIO_SECRET_KEY
    AWS_PROFILE: str = None
    AWS_REGION: str = ""
    SPARK_S3_BUCKET: str = "data"
    BULK_DOWNLOAD_S3_BUCKET_NAME: str = "bulk-download"
    DATABASE_DOWNLOAD_S3_BUCKET_NAME = "dti-usaspending-db"

    # Since this config values is built by composing others, we want to late/lazily-evaluate their values,
    # in case the declared value is overridden by a shell env var or .env file value
    AWS_S3_ENDPOINT: str = FACTORY_PROVIDED_VALUE  # See below validator-based factory

    @root_validator
    def _AWS_S3_ENDPOINT_factory(cls, values):
        def factory_func():
            return values["MINIO_HOST"] + ":" + values["MINIO_PORT"]

        return eval_default_factory_from_root_validator(cls, values, "AWS_S3_ENDPOINT", factory_func)

    AWS_STS_ENDPOINT: str = ""
