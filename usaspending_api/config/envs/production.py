########################################################################################################################
# Production runtime env configuration
# - Inherits all defaults from DefaultConfig in default.py
# - Overrides any default config variables with local runtime env specific values
# - Users may override local config vars values declared here by putting values in a git-ignored .env file alongside
#   docker-compose.yml
# - Set config variables to DefaultConfig.USER_SPECIFIC_OVERRIDE where there is expected to be a
#   user-provided a config value for a variable (e.g. in the ../.env file)
########################################################################################################################
from typing import ClassVar

from usaspending_api.config.envs.default import DefaultConfig


class ProductionConfig(DefaultConfig):
    """Config for a production runtime environment, which inherits and overrides from DefaultConfig

    See Also:
        Attributes inherited from or overridden from ``DefaultConfig``

    """

    # ==== [Global] ====
    ENV_CODE: ClassVar[str] = "prd"

    # ==== [AWS] ====
    AWS_PROFILE: str = None
    SPARK_S3_BUCKET = "dti-da-usaspending-spark-prod"

    # ==== [Spark] ====
    SPARK_MAX_JDBC_WRITER_CONNECTIONS: int = 16
