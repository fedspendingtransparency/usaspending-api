from datetime import datetime
import logging
import os
from time import time

from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from django.core.management import call_command
from django.db import connection

import django.db.models.base
from django.apps import apps


def models_with_custom_save():

    result = [m for m in apps.get_models() if m.save != django.db.models.base.Model.save]
    return 'Models with custom .save() methods:\n{}'.format('\n'.join(str(r) for r in result))


class Command(BaseCommand):
    """
    Run the `.save()` method on each instance of a named model.

    This applies any data fixes made within .save().
    """
    help = "Re-save all instances of named model."
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('model', nargs='+', help='app and model to be re-saved')

    def handle(self, *args, **options):

        if options['model']:
            try:
                model = apps.get_model(*options['model'])
                total = model.objects.count()
                start_time = time()
                for (i, instance) in enumerate(model.objects.all()):
                    if not (i % 1000):
                        print('{} of {} finished; elapsed: {} s'.format(i, total, time() - start_time))
                    instance.save()
                print('Finished {} records.  Elapsed: {} s'.format(total, time() - start_time))
            except (ValueError, LookupError):
                print('Model not found.  Specify app.model_name.')
                print(models_with_custom_save())
        else:
            print(models_with_custom_save())

