from django.db import connection
from retrying import retry
from multiprocessing import Process, JoinableQueue
from django import db
import logging
import csv
import codecs

from usaspending_api.common.long_to_terse import LONG_TO_TERSE_LABELS


# This class is a threaded data loader
# IMPLEMENTATION NOTE!!
# If you write a test that will use this loader, mark it with
# @pytest.mark.django_db(transaction=True)
# otherwise you may run into some concurrency issues!
class ThreadedDataLoader:
    # The threaded data loader requires a bit of set up, and explanation of the
    # parameters are below. Most parameter defaults are about where you'd want them:
    #   model_class - The class of the model each row of the file corresponds to
    #   processes - The number of processes to use. Default: 2 * number of machine cores
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
    def __init__(
        self,
        model_class,
        processes=None,
        field_map={},
        value_map={},
        collision_field=None,
        collision_behavior="update",
        pre_row_function=None,
        post_row_function=None,
        post_process_function=None,
        loghandler="console",
    ):
        self.logger = logging.getLogger(loghandler)
        self.model_class = model_class
        self.processes = processes
        if self.processes is None:
            # self.processes is set to 1 because the logger threading doesn't work, resulting in a bunch of garbage
            # being spat to console whenever this loader is used.
            # can change to self.processes = multiprocessing.cpu_count() * 2 or something else if we fix this issue
            self.processes = 1
            self.logger.info("Setting processes count to " + str(self.processes))
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
    def load_from_file(self, filepath, encoding="utf-8", remote_file=False):
        if not remote_file:
            self.logger.info("Started processing file " + filepath)

        # Create the Queue object - this will hold all the rows in the CSV
        row_queue = JoinableQueue(500)

        references = {
            "collision_field": self.collision_field,
            "collision_behavior": self.collision_behavior,
            "logger": self.logger,
            "pre_row_function": self.pre_row_function,
            "post_row_function": self.post_row_function,
            "fields": self.fields.copy(),
        }

        # Spawn our processes
        # We have to kill any DB connections before forking processes, as Django will want to share the single
        # connection with all processes and we don't want to have any deadlock/efficiency problems due to that
        db.connections.close_all()
        pool = []
        for i in range(self.processes):
            pool.append(
                DataLoaderThread(
                    "Process-" + str(len(pool)),
                    self.model_class,
                    row_queue,
                    self.field_map.copy(),
                    self.value_map.copy(),
                    references,
                )
            )

        for process in pool:
            process.start()

        if remote_file:
            csv_file = codecs.getreader("utf-8")(filepath["Body"])
            row_queue = self.csv_file_to_queue(csv_file, row_queue)
        else:
            with open(filepath, encoding=encoding) as csv_file:
                row_queue = self.csv_file_to_queue(csv_file, row_queue)

        for i in range(self.processes):
            row_queue.put(None)

        row_queue.join()

        for process in pool:
            process.terminate()

        if self.post_process_function is not None:
            self.post_process_function()

        self.logger.info("Finished processing all rows")

    def csv_file_to_queue(self, csv_file, row_queue):
        reader = csv.DictReader(csv_file)
        temp_row_queue = row_queue
        count = 0
        for row in reader:
            count = count + 1
            temp_row_queue.put(row)
            if count % 1000 == 0:
                self.logger.info("Queued row " + str(count))
        return temp_row_queue


class DataLoaderThread(Process):
    def __init__(self, name, model_class, data_queue, field_map, value_map, references):
        super(DataLoaderThread, self).__init__()
        self.name = name
        self.model_class = model_class
        self.data_queue = data_queue
        self.field_map = field_map
        self.value_map = value_map
        self.references = references

    def run(self):
        self.references["logger"].info("Starting " + self.name)
        connection.connect()
        self.process_data()
        self.references["logger"].info("Exiting " + self.name)

    def process_data(self):
        # Make sure we weren't flagged to exit
        while True:
            row = self.data_queue.get()
            # If row is none, we need to die.
            if row is None:
                self.data_queue.task_done()
                connection.close()
                return
            row = cleanse_values(row)
            # Grab the collision field
            update = False
            collision_instance = None
            model_instance = None
            if self.references["collision_field"] is not None:
                model_collision_field = self.references["collision_field"]
                data_collision_field = model_collision_field
                # If it's in the field map, we need to look it up
                if model_collision_field in self.field_map:
                    data_collision_field = self.field_map[model_collision_field]
                query = {model_collision_field: row[data_collision_field]}
                collision_instance = self.model_class.objects.filter(**query).first()

                if collision_instance is not None:
                    behavior = self.references["collision_behavior"]
                    if behavior == "delete":  # Delete the row from the data store and load new row
                        collision_instance.delete()
                    if behavior == "update":  # Update the row in the data store with new values from the CSV
                        update = True
                    if behavior == "skip":  # Skip the row
                        self.data_queue.task_done()
                        continue
                    if behavior == "skip_and_complain":  # Log a warning and skip the row
                        self.references["logger"].warning("Hit a collision on row %s" % (row))
                        self.data_queue.task_done()
                        continue
                    if behavior == "die":  # Raise an exception and cease execution
                        raise Exception("Hit collision on row %s" % (row))

            if not update:
                model_instance = self.model_class()
            else:
                model_instance = collision_instance

            try:
                if self.references["pre_row_function"] is not None:
                    self.references["pre_row_function"](row=row, instance=model_instance)

                self.load_data_into_model(
                    model_instance,
                    self.references["fields"],
                    self.field_map,
                    self.value_map,
                    row,
                    self.references["logger"],
                )

                # If we have a post row function, run it before saving
                if self.references["post_row_function"] is not None:
                    self.references["post_row_function"](row=row, instance=model_instance)

                model_instance.save()
            except SkipRowException as e:
                self.references["logger"].info(e)
            except Exception as e:
                self.references["logger"].error(e)

            self.data_queue.task_done()

    # Retry decorator to help resolve race conditions when constructing auxilliary objects
    @retry(stop_max_attempt_number=3, wait_fixed=10)
    def load_data_into_model(self, model_instance, fields, field_map, value_map, row, logger, as_dict=False):
        mod = model_instance

        if as_dict:
            mod = {}

        for model_field in fields:
            loaded = False

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
            # If we're not in the value map or the long_to_terse_labels, check the field map
            elif model_field in field_map:
                source_field = field_map[model_field]
            # If our field is the 'long form' field, we need to get what it maps to
            # in the data so we can map the data properly
            elif model_field in LONG_TO_TERSE_LABELS:
                source_field = LONG_TO_TERSE_LABELS[model_field]

            # If we haven't loaded via value map, load in the data using the source field
            if not loaded:
                if source_field in row:
                    self.store_value(mod, model_field, row[source_field])
                else:
                    pass
                    # logger.warning("Field " + source_field + " not available in data")

        if as_dict:
            return mod

    def store_value(self, model_instance_or_dict, field, value):
        if value is None:
            return
        if isinstance(model_instance_or_dict, dict):
            model_instance_or_dict[field] = value
        else:
            setattr(model_instance_or_dict, field, value)


def cleanse_values(row):
    """
    Remove textual quirks from CSV values.
    """
    row = {k: v.strip() for (k, v) in row.items()}
    row = {k: (None if v.lower() == "null" else v) for (k, v) in row.items()}
    return row


class SkipRowException(Exception):
    pass
