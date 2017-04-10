import random

# Simple router to randomly choose between reading from the source or read replicas


class ReadReplicaRouter(object):

    def db_for_read(self, model, **hints):
        return random.choice(['db_source', 'db_r1'])

    def db_for_write(self, model, **hints):
        """"
        write to source db only (bc read replicas)
        """
        return 'db_source'

    def allow_relation(self, obj1, obj2, **hints):
        db_list = ('db_source', 'db_r1')
        if obj1._state.db in db_list and obj2._state.db in db_list:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return True
