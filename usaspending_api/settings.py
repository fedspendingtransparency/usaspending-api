"""
For more information on this file: https://docs.djangoproject.com/en/1.11/topics/settings/
For the full list of settings and their values: https://docs.djangoproject.com/en/1.11/ref/settings/
"""

from django.utils.crypto import get_random_string
import dj_database_url
import os
import sys

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# User-specified limit on downloads should not be permitted beyond this
MAX_DOWNLOAD_LIMIT = 500000

# User-specified timeout limit for streaming downloads
DOWNLOAD_TIMEOUT_MIN_LIMIT = 10

# Default timeout for SQL statements in Django. Set to 5 min (in seconds).
DEFAULT_DB_TIMEOUT_IN_SECONDS = os.environ.get('DEFAULT_DB_TIMEOUT_IN_SECONDS') or 0
CONNECTION_MAX_SECONDS = 10

API_MAX_DATE = '2020-09-30'         # End of FY2020
API_MIN_DATE = '2000-10-01'         # Beginning of FY2001
API_SEARCH_MIN_DATE = '2007-10-01'  # Beginning of FY2008

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_random_string()

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = ['*']

# Define local flag to affect location of downloads
IS_LOCAL = True

# AWS Region for USAspending Infrastructure
USASPENDING_AWS_REGION = ""
if not USASPENDING_AWS_REGION:
    USASPENDING_AWS_REGION = os.environ.get('USASPENDING_AWS_REGION')

# AWS locations for CSV files
CSV_LOCAL_PATH = os.path.join(BASE_DIR, 'csv_downloads', '')
DOWNLOAD_ENV = ""
BULK_DOWNLOAD_LOCAL_PATH = os.path.join(BASE_DIR, 'bulk_downloads', '')

BULK_DOWNLOAD_S3_BUCKET_NAME = ""
BULK_DOWNLOAD_S3_REDIRECT_DIR = "generated_downloads"
BULK_DOWNLOAD_SQS_QUEUE_NAME = ""
MONTHLY_DOWNLOAD_S3_BUCKET_NAME = ""
MONTHLY_DOWNLOAD_S3_REDIRECT_DIR = "award_data_archive"
BROKER_AGENCY_BUCKET_NAME = ""
FPDS_BUCKET_NAME = ""
if not FPDS_BUCKET_NAME:
    FPDS_BUCKET_NAME = os.environ.get('FPDS_BUCKET_NAME')
DELETED_TRANSACTIONS_S3_BUCKET_NAME = ""
if not DELETED_TRANSACTIONS_S3_BUCKET_NAME:
    DELETED_TRANSACTIONS_S3_BUCKET_NAME = os.environ.get('DELETED_TRANSACTIONS_S3_BUCKET_NAME')
STATE_DATA_BUCKET = ""
if not STATE_DATA_BUCKET:
    STATE_DATA_BUCKET = os.environ.get('STATE_DATA_BUCKET')

ROSETTA_DICT_S3_PATH = "da-public-files/user_reference_docs/DATA Transparency Crosswalk.xlsx"
IDV_DOWNLOAD_README_FILE_PATH = os.path.join(BASE_DIR, "usaspending_api/data/idv_download_readme.txt")

# Elasticsearch
ES_HOSTNAME = ""
if not ES_HOSTNAME:
    ES_HOSTNAME = os.environ.get('ES_HOSTNAME')
TRANSACTIONS_INDEX_ROOT = os.environ.get('ES_TRX_ROOT') or 'future-transactions'
ES_TIMEOUT = 30
ES_REPOSITORY = ""

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.postgres',

    'debug_toolbar',
    'django_extensions',
    'rest_framework',
    'corsheaders',
    'rest_framework_tracking',
    'usaspending_api.common',
    'usaspending_api.etl',
    'usaspending_api.references',
    'usaspending_api.awards',
    'usaspending_api.accounts',
    'usaspending_api.submissions',
    'usaspending_api.financial_activities',
    'usaspending_api.api_docs',
    'usaspending_api.broker',
    'usaspending_api.download',
    'usaspending_api.bulk_download',
    'usaspending_api.recipient',
    'django_spaghetti',
    'simple_history'
]

INTERNAL_IPS = ()

DEBUG_TOOLBAR_CONFIG = {
    "SHOW_TOOLBAR_CALLBACK": lambda request: DEBUG
}

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'debug_toolbar.middleware.DebugToolbarMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'simple_history.middleware.HistoryRequestMiddleware',
    'usaspending_api.common.logging.LoggingMiddleware',
]

ROOT_URLCONF = 'usaspending_api.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': ['usaspending_api/templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'usaspending_api.wsgi.application'

# CORS Settings
CORS_ORIGIN_ALLOW_ALL = True  # Temporary while in development

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

# import an environment variable, DATABASE_URL
# see https://github.com/kennethreitz/dj-database-url for more info

DEFAULT_DB_OPTIONS = {
    'OPTIONS': {
        'options': '-c statement_timeout={0}'.format(DEFAULT_DB_TIMEOUT_IN_SECONDS * 1000)
    }
}

DATABASES = {
    'default': {**dj_database_url.config(conn_max_age=CONNECTION_MAX_SECONDS), **DEFAULT_DB_OPTIONS}
}

# read replica env vars... if not set, default DATABASE_URL will get used
# if only one set, this will error out (single DB should use DATABASE_URL)
if os.environ.get('DB_SOURCE') or os.environ.get('DB_R1'):
    DATABASES['db_source'] = dj_database_url.parse(os.environ.get('DB_SOURCE'), conn_max_age=CONNECTION_MAX_SECONDS)
    DATABASES['db_r1'] = dj_database_url.parse(os.environ.get('DB_R1'), conn_max_age=CONNECTION_MAX_SECONDS)
    DATABASE_ROUTERS = ['usaspending_api.routers.replicas.ReadReplicaRouter']

# import a second database connection for ETL, connecting to the data broker
# using the environemnt variable, DATA_BROKER_DATABASE_URL - only if it is set
if os.environ.get('DATA_BROKER_DATABASE_URL') and not sys.argv[1:2] == ['test']:
    DATABASES['data_broker'] = dj_database_url.parse(
        os.environ.get('DATA_BROKER_DATABASE_URL'),
        conn_max_age=CONNECTION_MAX_SECONDS
    )


# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

API_VERSION = 2

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.AllowAny',
    ],
    'DEFAULT_PAGINATION_CLASS': 'usaspending_api.common.pagination.UsaspendingPagination',
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'usaspending_api.common.renderers.DocumentApiRenderer',
        'usaspending_api.common.renderers.BrowsableAPIRendererWithoutForms',
    ),
}

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'usaspending_api/static/')
STATICFILES_DIRS = (os.path.join(BASE_DIR, 'usaspending_api/static_doc_files'),)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'specifics': {
            '()': "pythonjsonlogger.jsonlogger.JsonFormatter",
            'format': "%(asctime)s %(filename)s %(funcName)s %(levelname)s %(lineno)s %(module)s " +
                      "%(message)s %(name)s %(pathname)s"
        },
        'simpletime': {
            'format': "%(asctime)s - %(message)s",
            'datefmt': "%H:%M:%S"
        },
        'user_readable': {
            '()': "pythonjsonlogger.jsonlogger.JsonFormatter",
            'format': "%(timestamp)s %(status)s %(method)s %(path)s %(status_code)s %(remote_addr)s %(host)s " +
                      "%(response_ms)d %(message)s %(request)s %(traceback)s %(error_msg)s"
        }
    },
    'handlers': {
        'server': {
            'level': 'INFO',
            'class': 'logging.handlers.WatchedFileHandler',
            'filename': os.path.join(BASE_DIR, 'usaspending_api/logs/server.log'),
            'formatter': 'user_readable'
        },
        'console_file': {
            'level': 'INFO',
            'class': 'logging.handlers.WatchedFileHandler',
            'filename': os.path.join(BASE_DIR, 'usaspending_api/logs/console.log'),
            'formatter': 'specifics'
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simpletime'
        },
    },
    'loggers': {
        'server': {
            'handlers': ['server'],
            'level': 'INFO',
            'propagate': False,
        },
        'console': {
            'handlers': ['console', 'console_file'],
            'level': 'INFO',
            'propagate': False,
        }
    },
}

# If caches added or renamed, edit clear_caches in usaspending_api/etl/helpers.py
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'default-loc-mem-cache',
    },
    'locations': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'locations-loc-mem-cache',
    },
}

# Cache environment - 'local', 'disabled', or 'elasticache'
CACHE_ENVIRONMENT = 'disabled'

# Set up the appropriate elasticache for our environment
CACHE_ENVIRONMENTS = {
    # Elasticache settings are changed during deployment, or can be set manually
    'elasticache': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'ELASTICACHE-CONNECTION-STRING',
        'TIMEOUT': 'TIMEOUT-IN-SECONDS',
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            # Note: ELASTICACHE-MASTER-STRING is currently only used by Prod and will be removed in other environments.
            'MASTER_CACHE': 'ELASTICACHE-MASTER-STRING',
        },
    },
    'local': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'locations-loc-mem-cache',
    },
    'disabled': {
        'BACKEND': 'django.core.cache.backends.dummy.DummyCache',
    }
}

# Set the usaspending-cache to whatever our environment cache dictates
CACHES["usaspending-cache"] = CACHE_ENVIRONMENTS[CACHE_ENVIRONMENT]

# DRF extensions
REST_FRAMEWORK_EXTENSIONS = {
    # Not caching errors, these are logged to exceptions.log
    'DEFAULT_CACHE_ERRORS': False,
    # Default cache is usaspending-cache, which is set above based upon environment
    'DEFAULT_USE_CACHE': 'usaspending-cache',
    'DEFAULT_CACHE_KEY_FUNC': 'usaspending_api.common.cache.usaspending_key_func'
}

# Django spaghetti-and-meatballs (entity relationship diagram) settings
SPAGHETTI_SAUCE = {
    'apps': ['accounts', 'awards', 'financial_activities', 'references', 'submissions', 'recipient'],
    'show_fields': False,
    'exclude': {},
    'show_proxy': False,
}
