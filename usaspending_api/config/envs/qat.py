########################################################################################################################
# QAT runtime env configuration
# - Inherits all defaults from DefaultConfig in default.py
# - Overrides any default config variables with local runtime env specific values
# - Users may override local config vars values declared here by putting values in a git-ignored .env file alongside
#   docker-compose.yml
# - Set config variables to DefaultConfig.USER_SPECIFIC_OVERRIDE where there is expected to be a
#   user-provided a config value for a variable (e.g. in the ../.env file)
########################################################################################################################
from typing import ClassVar

from usaspending_api.config.envs.default import DefaultConfig


class QATConfig(DefaultConfig):
    """Config for a qat runtime environment, which inherits and overrides from DefaultConfig

    See Also:
        Attributes inherited from or overridden from ``DefaultConfig``

    """

    # ==== [Global] ====
    ENV_CODE: ClassVar[str] = "qat"

    # ==== [AWS] ====
    # In local dev env, default to NOT using AWS.
    # - For S3, MinIO will be used, and the AWS Endpoints defaulted below  will be used by MinIO to connect to "S3"
    #   locally.
    # - If you want to connect to AWS from your local dev env setup, for S3 as the backing object store of data,
    #   set this to True, and change the AWS endpoints/region to that of the targeted AWS account
    # - Then you MUST set your AWS creds (access/secret/token) by way of setting AWS_PROFILE env var (e.g. in your
    #   .env file)
    AWS_PROFILE: str = None
    SPARK_S3_BUCKET = "dti-da-usaspending-spark-qat"
