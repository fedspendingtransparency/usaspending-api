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

from usaspending_api.config.utils import USER_SPECIFIC_OVERRIDE
from pydantic.types import SecretStr

from usaspending_api.config.envs.default import DefaultConfig


class LocalConfig(DefaultConfig):
    """Config for a local runtime environment, which inherits and overrides from DefaultConfig

    See Also:
        Attributes inherited from or overridden from ``DefaultConfig``

    Attributes:
        POSTGRES_CLUSTER_DIR: Where docker persists postgres DB data
            - Should point to a path where data can be persistend beyond docker restarts,
              outside of the git source repository
        ES_CLUSTER_DIR: Where docker persists Elasticsearch shard data
            - Should point to a path where data can be persistend beyond docker restarts,
              outside of the git source repository
        MINIO_ACCESS_KEY: Access key for accessing S3 object data stored locally via MinIO
        MINIO_SECRET_KEY: Secret key for accessing S3 object data stored locally via MinIO
        MINIO_DATA_DIR: Where docker persists "object data" for S3 objects stored locally
            - Should point to a path where data can be persistend beyond docker restarts,
              outside of the git source repository
    """

    # ==== [Global] ====
    ENV_CODE: ClassVar[str] = "lcl"
    # Common credentials to share across services for convenience / ease on remembering
    _USASPENDING_USER = "usaspending"
    _USASPENDING_PASSWORD: SecretStr = "usaspender"

    # ==== [Postgres] ====
    POSTGRES_USER = _USASPENDING_USER
    POSTGRES_PASSWORD: SecretStr = _USASPENDING_PASSWORD

    # Change to host.docker.internal if you are running a local Postgres. Otherwise leave as-is, so
    # Docker will use the Postgres created by Compose.
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT = "5432"

    # Should point to a path where data can be persistend beyond docker restarts, outside of the git source repository
    POSTGRES_CLUSTER_DIR = USER_SPECIFIC_OVERRIDE

    # ==== [Elasticsearch] ====
    # Where to connect to elasticsearch.
    ES_SCHEME = "http"
    ES_HOST = "localhost"
    ES_PORT = "9200"

    # Should point to a path where data can be persistend beyond docker restarts, outside of the git source repository
    ES_CLUSTER_DIR = USER_SPECIFIC_OVERRIDE

    # ==== [MinIO] ====
    # MINIO_HOST = "host.docker.internal"
    MINIO_HOST = "localhost"
    MINIO_PORT = "9000"
    MINIO_ACCESS_KEY: SecretStr = _USASPENDING_USER  # likely overridden in .env
    MINIO_SECRET_KEY: SecretStr = _USASPENDING_PASSWORD  # likely overridden in .env
    # Should point to a path where data can be persistend beyond docker restarts, outside of the git source repository
    MINIO_DATA_DIR = USER_SPECIFIC_OVERRIDE

    # ==== [Spark] ====
    # Used for attaching to a spark-submit process to create a java_gateway for PySpark during unit test sessions
    # TODO: GET RID OF property(...) FIELDS! See note in comments in the tests/unit/test_config.py possibly look at
    #  pre/post load validator(...) or root_validator(...) functions as an alternative to composing other fields,
    #  or research more
    _PYSPARK_DRIVER_CONN_INFO_PATH: str = property(lambda self: self.PROJECT_LOG_DIR + "/pyspark_gateway_conn_info.log")

    # ==== [AWS] ====
    # In local dev env, default to NOT using AWS.
    # - For S3, MinIO will be used, and the AWS Endpoints defaulted below  will be used by MinIO to connect to "S3"
    #   locally.
    # - If you want to connect to AWS from your local dev env setup, for S3 as the backing object store of data,
    #   set this to True, and change the AWS endpoints/region to that of the targeted AWS account
    # - Then you MUST set your AWS creds (access/secret/token) by way of setting AWS_PROFILE env var (e.g. in your
    #   .env file)
    USE_AWS = False
    AWS_ACCESS_KEY: SecretStr = MINIO_ACCESS_KEY
    AWS_SECRET_KEY: SecretStr = MINIO_SECRET_KEY
    AWS_PROFILE: str = None
    AWS_REGION = ""
    AWS_S3_BUCKET = "data"
    # Since this config values is built by composing others, we want to late/lazily-evaluate their values,
    # in case the declared value is overridden by a shell env var or .env file value
    # A python property that calls a get-accessor function (or lambda) acts as a closure to provide this late-evaluation
    # TODO: GET RID OF property(...) FIELDS! See note in comments in the tests/unit/test_config.py possibly look at
    #  pre/post load validator(...) or root_validator(...) functions as an alternative to composing other fields,
    #  or research more
    AWS_S3_ENDPOINT = property(lambda self: self.MINIO_HOST + ":" + self.MINIO_PORT)
    AWS_STS_ENDPOINT = ""
