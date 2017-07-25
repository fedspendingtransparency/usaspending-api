import logging
import signal
from time import time

import django.db.models.base
from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import transaction


def models_with_custom_save():

    result = [m for m in apps.get_models() if m.save != django.db.models.base.Model.save]
    return 'Models with custom .save() methods:\n{}'.format('\n'.join(str(r) for r in result))


class Command(BaseCommand):
    """
    Run the `.save()` method on each instance of a named model.

    This applies any data fixes made within .save().
    Should only be necessary after there's a data modification there that
    needs to be applied to existing records.

    Argument: app.modelname, like `references.Location`
    """
    help = "Re-save all instances of named model.  Arg: app.modelname"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('model', nargs='+', help='app and model to be re-saved')

    @transaction.atomic
    def handle(self, *args, **options):

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception('Received interrupt signal. Aborting...')

        signal.signal(signal.SIGINT, signal_handler)

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
