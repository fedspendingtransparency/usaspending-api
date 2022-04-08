from usaspending_api.app_config.lcl import LocalAppConfig

# Environment Variable name which holds the app env code indicating the app environment to configure/deploy/run
APP_ENV_VAR = "APP_ENV"

# Capture manifest of all supported application environments
# env_type allows for multiple instantiations of different types of environments (e.g. stg01, stg02, or blue-green
# prod environments)
APP_ENVS = [
    {
        "env_type": "local",
        "code": LocalAppConfig.APP_ENV,
        "long_name": "local",
        "description": "Local Development Environment",
        "constructor": LocalAppConfig,
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
    # {
    #     "env_type": "staging",
    #     "code": "stg",
    #     "long_name": "staging",
    #     "description": "Staging Environment"
    # },
    # {
    #     "env_type": "production",
    #     "code": "prd",
    #     "long_name": "production",
    #     "description": "Production Environment"
    # },
]
