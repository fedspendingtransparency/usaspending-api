from usaspending_api.config import CONFIG


def test_postgres_dsn_config_populated_using_test_db(db):
    """Validate that when run with a Django test database (the ``db`` fixture), the database name is
    prefixed with test_ due to the pytest-django plugin"""
    pg_uri = CONFIG.DATABASE_URL
    assert "/test_" in pg_uri
