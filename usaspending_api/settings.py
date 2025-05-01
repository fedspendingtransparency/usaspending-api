"""
For more information on this file: https://docs.djangoproject.com/en/3.2/topics/settings/
For the full list of settings and their values: https://docs.djangoproject.com/en/3.2/ref/settings/
"""

import os
from pathlib import Path

import dj_database_url
from django.db import DEFAULT_DB_ALIAS
from django.utils.crypto import get_random_string

from usaspending_api.config import CONFIG

# All paths inside the project should be additive to REPO_DIR or APP_DIR
APP_DIR = Path(__file__).resolve().parent
REPO_DIR = APP_DIR.parent

# Row-limited download limit
MAX_DOWNLOAD_LIMIT = 500000

# Timeout limit for streaming downloads
DOWNLOAD_TIMEOUT_MIN_LIMIT = 10

# Data Dictionary retry settings
DATA_DICTIONARY_DOWNLOAD_RETRY_COUNT = 3
DATA_DICTIONARY_DOWNLOAD_RETRY_COOLDOWN = 15

# Default timeout for SQL statements in Django
DEFAULT_DB_TIMEOUT_IN_SECONDS = int(os.environ.get("DEFAULT_DB_TIMEOUT_IN_SECONDS", 0))
DOWNLOAD_DB_TIMEOUT_IN_HOURS = 6
CONNECTION_MAX_SECONDS = 10

# Default type for when a Primary Key is not specified
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"
# Default text search config setting when dealing with tsvectors
DEFAULT_TEXT_SEARCH_CONFIG = "pg_catalog.simple"

# Value is set per environment using formula: ( TOTAL_MEMORY * 0.25 ) / MAX_CONNECTIONS;
# MAX_CONNECTIONS in this case refers to those serving downloads
DOWNLOAD_DB_WORK_MEM_IN_MB = os.environ.get("DOWNLOAD_DB_WORK_MEM_IN_MB", 128)

API_MAX_DATE = "2027-09-30"  # End of FY2027
API_MIN_DATE = "2000-10-01"  # Beginning of FY2001
API_SEARCH_MIN_DATE = "2007-10-01"  # Beginning of FY2008

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_random_string(length=12)

# SECURITY WARNING: don't run with debug turned on in production!
# Defaults to False, unless DJANGO_DEBUG env var is set to a truthy value
DEBUG = os.environ.get("DJANGO_DEBUG", "").lower() in ["true", "1", "yes"]

HOST = "localhost:3000"
ALLOWED_HOSTS = ["*"]

# Define local flag to affect location of downloads
IS_LOCAL = True

# Indicates which environment is sending traces to Grafana.
# This will be overwritten by Ansible
TRACE_ENV = "unspecified"

# How to handle downloads locally
# True: process it right away by the API;
# False: leave the message in the local file-backed queue to be picked up and processed by the bulk-download container
RUN_LOCAL_DOWNLOAD_IN_PROCESS = os.environ.get("RUN_LOCAL_DOWNLOAD_IN_PROCESS", "").lower() not in ["false", "0", "no"]

# AWS Region for USAspending Infrastructure
USASPENDING_AWS_REGION = ""
if not USASPENDING_AWS_REGION:
    USASPENDING_AWS_REGION = os.environ.get("USASPENDING_AWS_REGION")

# AWS locations for CSV files
CSV_LOCAL_PATH = str(REPO_DIR / "csv_downloads") + "/"
DOWNLOAD_ENV = ""
BULK_DOWNLOAD_LOCAL_PATH = str(REPO_DIR / "bulk_downloads") + "/"

DATABASE_DOWNLOAD_S3_BUCKET_NAME = CONFIG.DATABASE_DOWNLOAD_S3_BUCKET_NAME
BULK_DOWNLOAD_S3_BUCKET_NAME = CONFIG.BULK_DOWNLOAD_S3_BUCKET_NAME
BULK_DOWNLOAD_S3_REDIRECT_DIR = "generated_downloads"
BULK_DOWNLOAD_SQS_QUEUE_NAME = ""
DATABASE_DOWNLOAD_S3_REDIRECT_DIR = "database_download"
MONTHLY_DOWNLOAD_S3_BUCKET_NAME = ""
MONTHLY_DOWNLOAD_S3_REDIRECT_DIR = "award_data_archive"
BROKER_AGENCY_BUCKET_NAME = ""
UNLINKED_AWARDS_DOWNLOAD_REDIRECT_DIR = "unlinked_awards_downloads"

# This list contains any abnormal characters in agency names
# This list is important to track which characters we need to replace in
# the agency name before the name can be used in a file name
UNLINKED_AWARDS_AGENCY_NAME_CHARS_TO_REPLACE = [".", " ", "/", "(", ")", "-", "&", "'"]

############################################################
# Note 2020/02/21
# FPDS_BUCKET_NAME and DELETED_TRANSACTIONS_S3_BUCKET_NAME are used in different
#  places with the same string value in deployed envs
# Merging together into a new variable: DELETED_TRANSACTION_JOURNAL_FILES
# After the new variable reaches master, work with OPS to discard old variables
FPDS_BUCKET_NAME = ""
DELETED_TRANSACTIONS_S3_BUCKET_NAME = ""
DELETED_TRANSACTION_JOURNAL_FILES = ""

if not FPDS_BUCKET_NAME:
    FPDS_BUCKET_NAME = os.environ.get("FPDS_BUCKET_NAME")
if not DELETED_TRANSACTIONS_S3_BUCKET_NAME:
    DELETED_TRANSACTIONS_S3_BUCKET_NAME = os.environ.get("DELETED_TRANSACTIONS_S3_BUCKET_NAME")
if not DELETED_TRANSACTION_JOURNAL_FILES:
    DELETED_TRANSACTION_JOURNAL_FILES = (
        os.environ.get("DELETED_TRANSACTION_JOURNAL_FILES") or FPDS_BUCKET_NAME or DELETED_TRANSACTIONS_S3_BUCKET_NAME
    )

############################################################

STATE_DATA_BUCKET = ""
if not STATE_DATA_BUCKET:
    STATE_DATA_BUCKET = os.environ.get("STATE_DATA_BUCKET")

# Download URLs
FILES_SERVER_BASE_URL = ""
SERVER_BASE_URL = ""

if not FILES_SERVER_BASE_URL:
    # This logic should be re-worked following DNS change for lower environments
    FILES_SERVER_BASE_URL = os.environ.get(
        "FILES_SERVER_BASE_URL",
        f"https://files{'-nonprod' if DOWNLOAD_ENV not in ('production', 'staging') else ''}.usaspending.gov",
    )
    SERVER_BASE_URL = FILES_SERVER_BASE_URL[FILES_SERVER_BASE_URL.find(".") + 1 :]

AGENCY_DOWNLOAD_URL = f"{FILES_SERVER_BASE_URL}/reference_data/agency_codes.csv"
DATA_DICTIONARY_DOWNLOAD_URL = f"{FILES_SERVER_BASE_URL}/docs/Data_Dictionary_Crosswalk.xlsx"

# S3 Bucket and Key to retrieve the Data Dictionary
DATA_DICTIONARY_S3_BUCKET_NAME = f"dti-da-public-files-{'nonprod' if CONFIG.ENV_CODE not in ('prd', 'stg') else 'prod'}"
DATA_DICTIONARY_S3_KEY = "user_reference_docs/Data_Dictionary_Crosswalk.xlsx"

# Local download files
IDV_DOWNLOAD_README_FILE_PATH = str(APP_DIR / "data" / "idv_download_readme.txt")
ASSISTANCE_DOWNLOAD_README_FILE_PATH = str(APP_DIR / "data" / "AssistanceSummary_download_readme.txt")
CONTRACT_DOWNLOAD_README_FILE_PATH = str(APP_DIR / "data" / "ContractSummary_download_readme.txt")
COVID19_DOWNLOAD_README_FILE_PATH = str(APP_DIR / "data" / "COVID-19_download_readme.txt")
UNLINKED_AWARDS_DOWNLOAD_README_FILE_PATH = str(APP_DIR / "data" / "unlinked_awards_instructions_readme.txt")
COVID19_DOWNLOAD_FILENAME_PREFIX = "COVID-19_Profile"

# Elasticsearch
ES_HOSTNAME = ""
if not ES_HOSTNAME:
    ES_HOSTNAME = os.environ.get("ES_HOSTNAME")

ES_AWARDS_ETL_VIEW_NAME = "award_delta_view"
ES_AWARDS_MAX_RESULT_WINDOW = 50000
ES_AWARDS_NAME_SUFFIX = "awards"
ES_AWARDS_QUERY_ALIAS_PREFIX = "award-query"
ES_AWARDS_WRITE_ALIAS = "award-load-alias"

ES_SUBAWARD_ETL_VIEW_NAME = "subaward_es_view"
ES_SUBAWARD_NAME_SUFFIX = "subaward"
ES_SUBAWARD_MAX_RESULT_WINDOW = 50000
ES_SUBAWARD_QUERY_ALIAS_PREFIX = "subaward-query"
ES_SUBAWARD_WRITE_ALIAS = "subaward-load-alias"

ES_TRANSACTIONS_ETL_VIEW_NAME = "transaction_delta_view"
ES_TRANSACTIONS_MAX_RESULT_WINDOW = 50000
ES_TRANSACTIONS_NAME_SUFFIX = "transactions"
ES_TRANSACTIONS_QUERY_ALIAS_PREFIX = "transaction-query"
ES_TRANSACTIONS_WRITE_ALIAS = "transaction-load-alias"

ES_RECIPIENTS_ETL_VIEW_NAME = "recipient_profile_delta_view"
ES_RECIPIENTS_MAX_RESULT_WINDOW = 50000
ES_RECIPIENTS_NAME_SUFFIX = "recipients"
ES_RECIPIENTS_QUERY_ALIAS_PREFIX = "recipient-query"
ES_RECIPIENTS_WRITE_ALIAS = "recipient-load-alias"

ES_LOCATIONS_ETL_VIEW_NAME = "location_delta_view"
ES_LOCATIONS_MAX_RESULT_WINDOW = 50000
ES_LOCATIONS_NAME_SUFFIX = "locations"
ES_LOCATIONS_QUERY_ALIAS_PREFIX = "location-query"
ES_LOCATIONS_WRITE_ALIAS = "location-load-alias"

ES_TIMEOUT = 90
ES_REPOSITORY = ""
ES_ROUTING_FIELD = "recipient_agg_key"

# Grants API
GRANTS_API_KEY = os.environ.get("GRANTS_API_KEY")
GRANTS_URL = "https://apply07.grants.gov/grantsws/rest/opportunities/search/cfda/totals"

# Applications https://docs.djangoproject.com/en/3.2/ref/settings/#installed-apps
INSTALLED_APPS = [
    # Built-in
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.postgres",
    "django.contrib.staticfiles",
    # Third-party
    "corsheaders",
    "debug_toolbar",
    "django_extensions",
    "django_spaghetti",
    "opentelemetry",
    "rest_framework",
    "rest_framework_tracking",
    # Project applications
    "usaspending_api.accounts",
    "usaspending_api.agency",
    "usaspending_api.api_docs",
    "usaspending_api.awards",
    "usaspending_api.broker",
    "usaspending_api.bulk_download",
    "usaspending_api.common",
    "usaspending_api.database_scripts.job_archive",
    "usaspending_api.disaster",
    "usaspending_api.download",
    "usaspending_api.etl",
    "usaspending_api.financial_activities",
    "usaspending_api.recipient",
    "usaspending_api.references",
    "usaspending_api.reporting",
    "usaspending_api.search",
    "usaspending_api.submissions",
    "usaspending_api.transactions",
]

INTERNAL_IPS = ()

DEBUG_TOOLBAR_CONFIG = {"SHOW_TOOLBAR_CALLBACK": lambda request: DEBUG}

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "debug_toolbar.middleware.DebugToolbarMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "usaspending_api.common.logging.LoggingMiddleware",
]

ROOT_URLCONF = "usaspending_api.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": ["usaspending_api/templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

WSGI_APPLICATION = "usaspending_api.wsgi.application"

# CORS Settings
CORS_ORIGIN_ALLOW_ALL = True  # Temporary while in development

# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

# import an environment variable, DATABASE_URL
# see https://github.com/kennethreitz/dj-database-url for more info


def _configure_database_connection(environment_variable, test_options=None):
    """
    Configure a Django database connection... configuration.  environment_variable is the name of
    the operating system environment variable that contains the database connection string or DSN
    """
    if test_options is None:
        test_options = {}
    default_options = {"options": "-c statement_timeout={0}".format(DEFAULT_DB_TIMEOUT_IN_SECONDS * 1000)}
    config = dj_database_url.parse(os.environ.get(environment_variable), conn_max_age=CONNECTION_MAX_SECONDS)
    config["OPTIONS"] = {**config.setdefault("OPTIONS", {}), **default_options}
    config["TEST"] = {**test_options, "SERIALIZE": False}
    return config


# If DB_SOURCE is set, use it as our default database, otherwise use dj_database_url.DEFAULT_ENV
# (which is "DATABASE_URL" by default). Generally speaking, DB_SOURCE is used to support server
# environments that support the API/website and docker-compose local setup whereas DATABASE_URL
# is used for development and operational environments (Jenkins primarily). If DB_SOURCE is provided,
# then DB_R1 (read replica) must also be provided.
if os.environ.get("DB_SOURCE"):
    if not os.environ.get("DB_R1"):
        raise EnvironmentError("DB_SOURCE environment variable defined without DB_R1")
    DATABASES = {
        DEFAULT_DB_ALIAS: _configure_database_connection("DB_SOURCE"),
        "db_r1": _configure_database_connection("DB_R1"),
    }
    DATABASE_ROUTERS = ["usaspending_api.routers.replicas.ReadReplicaRouter"]
elif os.environ.get(dj_database_url.DEFAULT_ENV):
    DATABASES = {
        DEFAULT_DB_ALIAS: _configure_database_connection(dj_database_url.DEFAULT_ENV),
    }
    DATABASE_ROUTERS = ["usaspending_api.routers.replicas.DefaultOnlyRouter"]
else:
    raise EnvironmentError(
        "Either {} or DB_SOURCE/DB_R1 environment variable must be defined".format(dj_database_url.DEFAULT_ENV)
    )

# Initializing download DB as connection string (DOWNLOAD_DATABASE_URL) for PSQL command and
# Django connection (DATABASES["db_download"]) to allow for Queryset directly against DB used for downloads
DOWNLOAD_DB_ALIAS = "db_download"
if os.environ.get("DOWNLOAD_DATABASE_URL"):
    DOWNLOAD_DATABASE_URL = os.environ.get("DOWNLOAD_DATABASE_URL")
    DATABASES[DOWNLOAD_DB_ALIAS] = _configure_database_connection(
        "DOWNLOAD_DATABASE_URL", test_options={"MIRROR": DEFAULT_DB_ALIAS}
    )

# import a second database connection for ETL, connecting to data broker
# using the environment variable, DATA_BROKER_DATABASE_URL - only if it is set
DATA_BROKER_DB_ALIAS = "data_broker"
if os.environ.get("DATA_BROKER_DATABASE_URL"):
    DATABASES[DATA_BROKER_DB_ALIAS] = _configure_database_connection("DATA_BROKER_DATABASE_URL")

DATA_BROKER_DBLINK_NAME = "broker_server"

# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

API_VERSION = 2

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.AllowAny"],
    "DEFAULT_PAGINATION_CLASS": "usaspending_api.common.pagination.UsaspendingPagination",
    "DEFAULT_RENDERER_CLASSES": (
        "rest_framework.renderers.JSONRenderer",
        "usaspending_api.common.renderers.DocumentAPIRenderer",
        "usaspending_api.common.renderers.BrowsableAPIRendererWithoutForms",
    ),
    "EXCEPTION_HANDLER": "usaspending_api.common.custom_exception_handler.custom_exception_handler",
}

# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = "/static/"
STATIC_ROOT = str(APP_DIR / "static/") + "/"
STATICFILES_DIRS = (str(APP_DIR / "static_doc_files") + "/",)

# Setup dir for console.log if it doesn't exist
logs_dir = APP_DIR / "logs"
console_log_file_path = logs_dir / "console.log"
logs_dir.mkdir(parents=True, exist_ok=True)
console_log_file_path.touch(exist_ok=True)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "specifics": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(filename)s %(funcName)s %(levelname)s %(lineno)s %(module)s "
            + "%(message)s %(name)s %(pathname)s",
        },
        "simpletime": {"format": "%(asctime)s - %(message)s", "datefmt": "%H:%M:%S"},
        "user_readable": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(timestamp)s %(status)s %(method)s %(path)s %(status_code)s %(remote_addr)s %(host)s "
            + "%(response_ms)d %(message)s %(request)s %(traceback)s %(error_msg)s",
        },
        "detailed": {"format": "[%(asctime)s] [%(levelname)s] - %(message)s", "datefmt": "%Y/%m/%d %H:%M:%S (%Z)"},
        # tracing is used for some spark jobs' logs indirectly
        "tracing": {
            "format": "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s:%(lineno)s: %(message)s",
            "datefmt": "%y/%m/%d %H:%M:%S",
        },
    },
    "handlers": {
        "server": {
            "level": "DEBUG",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": str(APP_DIR / "logs" / "server.log"),
            "formatter": "user_readable",
        },
        "console_file": {
            "level": "DEBUG",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": str(APP_DIR / "logs" / "console.log"),
            "formatter": "specifics",
        },
        "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "simpletime"},
        "script": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "detailed"},
    },
    "loggers": {
        # The root logger; i.e. "all modules"
        "": {"handlers": ["console", "console_file"], "level": "WARNING", "propagate": False},
        # The "root" logger for all usaspending_api modules
        "usaspending_api": {"handlers": ["console", "console_file"], "level": "DEBUG", "propagate": False},
        # Logger for Django API requests via middleware. See logging.py
        "server": {"handlers": ["server"], "level": "DEBUG", "propagate": False},
        # Catch-all logger (over)used for non-Django-API commands that output to the console
        "console": {"handlers": ["console", "console_file"], "level": "DEBUG", "propagate": False},
        # More-verbose logger for ETL scripts
        "script": {"handlers": ["script"], "level": "DEBUG", "propagate": False},
        # Logger used to specifically record exceptions
        "exceptions": {"handlers": ["console", "console_file"], "level": "ERROR", "propagate": False},
    },
}

if DEBUG:
    # make all loggers use the console when in debug
    for logger in LOGGING["loggers"]:
        if "console" not in LOGGING["loggers"][logger]["handlers"]:
            LOGGING["loggers"][logger]["handlers"] += ["console"]

    LOGGING["handlers"]["console"]["level"] = "DEBUG"
    LOGGING["loggers"]["django.db.backends"] = {"handlers": ["console"], "level": "DEBUG", "propagate": False}


# If caches added or renamed, edit clear_caches in usaspending_api/etl/helpers.py
CACHES = {
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "default-loc-mem-cache"},
    "locations": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "locations-loc-mem-cache"},
}

# Cache environment - 'local', 'disabled', or 'elasticache'
CACHE_ENVIRONMENT = "disabled"

# Set up the appropriate elasticache for our environment
CACHE_ENVIRONMENTS = {
    # Elasticache settings are changed during deployment, or can be set manually
    "elasticache": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "ELASTICACHE-CONNECTION-STRING",
        "TIMEOUT": "TIMEOUT-IN-SECONDS",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            # Note: ELASTICACHE-MASTER-STRING is currently only used by Prod and will be removed in other environments.
            "MASTER_CACHE": "ELASTICACHE-MASTER-STRING",
        },
    },
    "local": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "locations-loc-mem-cache"},
    "disabled": {"BACKEND": "django.core.cache.backends.dummy.DummyCache"},
}

# Set the usaspending-cache to whatever our environment cache dictates
CACHES["usaspending-cache"] = CACHE_ENVIRONMENTS[CACHE_ENVIRONMENT]

# DRF extensions
REST_FRAMEWORK_EXTENSIONS = {
    # Not caching errors, these are logged to exceptions.log
    "DEFAULT_CACHE_ERRORS": False,
    # Default cache is usaspending-cache, which is set above based upon environment
    "DEFAULT_USE_CACHE": "usaspending-cache",
    "DEFAULT_CACHE_KEY_FUNC": "usaspending_api.common.cache.usaspending_key_func",
}

# Django spaghetti-and-meatballs (entity relationship diagram) settings
SPAGHETTI_SAUCE = {
    "apps": ["accounts", "awards", "financial_activities", "references", "submissions", "recipient"],
    "show_fields": False,
    "exclude": {},
    "show_proxy": False,
}

SESSION_COOKIE_SECURE = True

CSRF_COOKIE_SECURE = True
