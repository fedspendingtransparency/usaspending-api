from usaspending_api.config.envs.local import LocalConfig
from usaspending_api.config.envs.production import ProductionConfig
from usaspending_api.config.envs.qat import QATConfig
from usaspending_api.config.envs.sandbox import SandboxConfig
from usaspending_api.config.envs.staging import StagingConfig

__all__ = [
    "ENV_CODE_VAR",
    "ENVS",
]

# Environment Variable name which holds the runtime env code indicating the runtime environment to configure/deploy/run
ENV_CODE_VAR = "ENV_CODE"

# Capture manifest of all supported runtime environments
# env_type allows for multiple instantiations of different types of runtime environments
# (e.g. stg01, stg02, or blue-green prod environments)
ENVS = [
    {
        "env_type": "local",
        "code": LocalConfig.ENV_CODE,
        "long_name": "local",
        "description": "Local Development Environment",
        "constructor": LocalConfig,
    },
    {
        "env_type": "development",
        "code": SandboxConfig.ENV_CODE,
        "long_name": "sandbox",
        "description": "Sandbox Development Environment",
        "constructor": SandboxConfig,
    },
    {
        "env_type": "testing",
        "code": QATConfig.ENV_CODE,
        "long_name": "qat",
        "description": "Quality Assurance Testing Environment",
        "constructor": QATConfig,
    },
    {
        "env_type": "staging",
        "code": StagingConfig.ENV_CODE,
        "long_name": "staging",
        "description": "Staging Environment",
        "constructor": StagingConfig,
    },
    {
        "env_type": "production",
        "code": ProductionConfig.ENV_CODE,
        "long_name": "production",
        "description": "Production Environment",
        "constructor": ProductionConfig,
    },
    # {
    #     "env_type": "development",
    #     "code": "dev",
    #     "long_name": "development",
    #     "description": "Shared Development Environment"
    # },
    # {
    #     "env_type": "build",
    #     "code": "ci",
    #     "long_name": "continuous_integration",
    #     "description": "Environment created at build time, to support continuous integration tests"
    # },
    # {
    #     "env_type": "test",
    #     "code": "qa",
    #     "long_name": "quality_assurance",
    #     "description": "Quality Assurance Testing Environment"
    # },
]
