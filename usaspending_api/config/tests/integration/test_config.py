from usaspending_api.config import CONFIG


def test_postgres_dsn_config_populated_using_test_db(db):
    """Validate assumptions about parts of the PostgresDsn object getting populated by the default LocalConfig
    config var values AND that when run with a Django test database (the ``db`` fixture), the database name is
    prefixed with test_ due to the pytest-django plugin"""
    pg_dsn = CONFIG.POSTGRES_DSN
    assert pg_dsn.host is not None
    assert pg_dsn.port is not None
    assert pg_dsn.user is not None
    assert pg_dsn.password is not None
    assert pg_dsn.path is not None
    assert pg_dsn.scheme is not None
    assert pg_dsn.path.startswith("/test_")
