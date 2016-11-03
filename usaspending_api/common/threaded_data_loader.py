from django.db.models import Q
from django.conf import settings
from django.db import connection
from retrying import retry
import time
import logging
import csv
import queue
import threading


# This class is a threaded data loader
class ThreadedDataLoader():
    # The threaded data loader requires a bit of set up, and explanation of the
    # parameters are below. Most parameter defaults are about where you'd want them:
    #   model_class - The class of the model each row of the file corresponds to
    #   threads - The number of threads to use. Default: 10
    #   field_map - A dict map from the CSV's columns to model field names. Default: Empty
    #   value_map - A dict map of default or processed values to use. Keys should be model fields
    #               To process values, create a lambda function that accepts a parameter which is
    #               a dict representing the row. i.e. lambda row: datetime(row['published date'])
    #               (Theoretically you can use some arbitrary method, as well, for complex processes)
    #   collision_field - The field with which to collide upon, usually some PK or unique identifier. String
    #                     Default: None
    #   collision_behavior - A string representing the collision behavior, which occurs when a collision is detected
    #                       Options:
    #                           delete - Delete the row from the data store and load new row
    #                           update - Update the row in the data store with new values from the CSV
    #                           skip - Skip the row
    #                           skip_and_complain - Log a warning and skip the row
    #                           die - Raise an exception and cease execution
    #   post_row_function - A function to call after every row. Should support parameters of row and instance
    #                       i.e. def do_this_after_a_row(row=None, instance=None)
    #   pre_row_function - Like post_row_function, but before the model class is updated
    #   post_process_function - A function to call when all rows have been processed, uses the same
    #                           function parameters as post_row_function
    def __init__(self, model_class, threads=10, field_map={}, value_map={}, collision_field=None, collision_behavior='update', pre_row_function=None, post_row_function=None, post_process_function=None):
        self.logger = logging.getLogger('console')
        self.model_class = model_class
        self.threads = threads
        self.field_map = field_map
        self.value_map = value_map
        self.collision_field = collision_field
        self.collision_behavior = collision_behavior
        self.pre_row_function = pre_row_function
        self.post_row_function = post_row_function
        self.post_process_function = post_process_function
        self.fields = [field.name for field in self.model_class._meta.get_fields()]

    # Loads data from a file using parameters set during creation of the loader
    # The filepath parameter should be the string location of the file for use with open()
    def load_from_file(self, filepath):
        self.logger.info('Started processing file ' + filepath)
        # Create the Queue object - this will hold all the rows in the CSV
        row_queue = queue.Queue(maxsize=100)
        self.exit_flag = False

        # Spawn our threads
        pool = []
        while len(pool) < self.threads:
            thread = DataLoaderThread("Thread-" + str(len(pool)), row_queue, self)
            thread.start()
            pool.append(thread)

        with open(filepath) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row_queue.put(row)

        while not row_queue.empty():
            pass

        self.exit_flag = True

        for thread in pool:
            thread.join()

        if self.post_process_function is not None:
            self.post_process_function()

        self.logger.info('Finished processing all threads')


class DataLoaderThread(threading.Thread):
    def __init__(self, name, data_queue, loader):
        threading.Thread.__init__(self)
        self.name = name
        self.data_queue = data_queue
        self.loader = loader

    def run(self):
        self.loader.logger.info("Starting " + self.name)
        self.process_data()
        self.loader.logger.info("Exiting " + self.name)

    def process_data(self):
        # Make sure we weren't flagged to exit
        while not self.loader.exit_flag:
            if not self.data_queue.empty():
                row = self.data_queue.get()
                # Grab the collision field
                update = False
                collision_instance = None
                model_instance = None
                if self.loader.collision_field is not None:
                    model_collision_field = self.loader.collision_field
                    data_collision_field = model_collision_field
                    # If it's a long label, we need to look it up to map to data
                    if model_collision_field in settings.LONG_TO_TERSE_LABELS:
                        data_collision_field = settings.LONG_TO_TERSE_LABELS[model_collision_field]
                    # If it's in the field map, we need to look it up
                    elif model_collision_field in self.loader.field_map:
                        data_collision_field = self.loader.field_map[model_collision_field]
                    query_object = Q({model_collision_field: row[data_collision_field]})
                    collision_instance = self.loader.model_class.objects.filter(query_object).first()

                    if collision_instance is not None:
                        behavior = self.loader.collision_behavior
                        if behavior is "delete":  # Delete the row from the data store and load new row
                            collision_instance.delete()
                        if behavior is "update":  # Update the row in the data store with new values from the CSV
                            update = True
                        if behavior is "skip":  # Skip the row
                            continue
                        if behavior is "skip_and_complain":  # Log a warning and skip the row
                            self.loader.logger.warning('Hit a collision on row %s' % (row))
                            continue
                        if behavior is "die":  # Raise an exception and cease execution
                            raise Exception("Hit collision on row %s" % (row))

                if not update:
                    model_instance = self.loader.model_class()
                else:
                    model_instance = collision_instance

                if self.loader.pre_row_function is not None:
                    self.loader.pre_row_function(row=row, instance=model_instance)

                try:
                    processed_row = self.load_data_into_model(model_instance,
                                                              self.loader.fields,
                                                              self.loader.field_map,
                                                              self.loader.value_map,
                                                              row,
                                                              logger=self.loader.logger)
                except Exception as e:
                    self.loader.logger.error(e)
                # If we have a post row function, run it before saving
                if self.loader.post_row_function is not None:
                    self.loader.post_row_function(row=row, instance=model_instance)

                model_instance.save()
            time.sleep(1)
        connection.close()

    # Retry decorator to help resolve race conditions when constructing auxilliary objects
    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def load_data_into_model(self, model_instance, fields, field_map, value_map, row, logger, as_dict=False):
        mod = model_instance
        loaded = False

        if as_dict:
            mod = {}

        for model_field in fields:
            source_field = model_field
            # If our model_field is present in the value map, use that value
            if model_field in value_map:
                # Check if the stored value is a function
                if callable(value_map[model_field]):
                    value = value_map[model_field](row)
                    self.store_value(mod, model_field, value)
                    loaded = True
                else:
                    self.store_value(mod, model_field, value_map[model_field])
                    loaded = True
            # If our field is the 'long form' field, we need to get what it maps to
            # in the data so we can map the data properly
            elif model_field in settings.LONG_TO_TERSE_LABELS:
                source_field = settings.LONG_TO_TERSE_LABELS[model_field]
            # If we're not in the value map or the long_to_terse_labels, check the field map
            elif model_field in field_map:
                source_field = field_map[model_field]

            # If we haven't loaded via value map, load in the data using the source field
            if not loaded:
                if source_field in row:
                    self.store_value(mod, model_field, row[source_field])
                else:
                    logger.warning('Could not find any data for field ' + model_field)

        if as_dict:
            return mod

    def store_value(self, model_instance_or_dict, field, value):
        if value is None:
            return
        if isinstance(model_instance_or_dict, dict):
            model_instance_or_dict[field] = value
        else:
            setattr(model_instance_or_dict, field, value)
