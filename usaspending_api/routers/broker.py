class BrokerRouter(object):
    """
    Ensures that no migrations are applied to the Broker DB, even though it may appear in the settings.DATABASES
    dictionary in settings.py.

    This will also prohibit migrations when using this database and a connection to it when running tests
    """

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == "data_broker":
            return False
        return None  # i.e., the router has "no opinion" for other DBs
