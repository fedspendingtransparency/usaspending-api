import logging
import signal
from time import time

import django.db.models.base
from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import transaction


def models_with_custom_save():

    result = [m for m in apps.get_models() if m.save != django.db.models.base.Model.save]
    return "Models with custom .save() methods:\n{}".format("\n".join(str(r) for r in result))


class Command(BaseCommand):
    """
    Run the `.save()` method on each instance of a named model.

    This applies any data fixes made within .save().
    Should only be necessary after there's a data modification there that
    needs to be applied to existing records.

    Argument: app.modelname, like `references.Location`
    """

    help = "Re-save all instances of named model.  Arg: app.modelname"
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument("model", nargs="+", help="app and model to be re-saved")
        parser.add_argument(
            "-i", "--id", nargs="+", default=[], help="(optional) re-save only rows with these primary keys"
        )

    @transaction.atomic
    def handle(self, *args, **options):
        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        if options["model"]:
            try:
                model = apps.get_model(*options["model"])
            except (ValueError, LookupError):
                print(models_with_custom_save())
                raise LookupError("Model not found.  Specify app.model_name.")

            if options["id"] == []:
                self.resave_all(model=model)
            else:
                self.resave_one(model=model, pks=options["id"])
        else:
            print(models_with_custom_save())

    def resave_all(self, model):
        total = model.objects.count()
        start_time = time()
        for (i, instance) in enumerate(model.objects.all()):
            if not (i % 1000):
                print("{} of {} finished; elapsed: {} s".format(i, total, time() - start_time))
            instance.save()
        print("Finished {} records.  Elapsed: {} s".format(total, time() - start_time))

    def resave_one(self, model, pks):
        for pk in pks:
            instance = model.objects.get(pk=pk)
            if instance:
                instance.save()
            else:
                raise KeyError("%s with primary key %s not found" % (model.name, pk))
