import random


class ReadReplicaRouter(object):
    def db_for_read(self, model, **hints):
        return random.choice(['db_r1'])

    def db_for_write(self, model, **hints):
        """"
        write to source db only (bc read replicas)
        """
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return True
