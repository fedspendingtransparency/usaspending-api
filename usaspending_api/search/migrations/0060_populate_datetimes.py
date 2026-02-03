from functools import partial
from datetime import datetime, time
from django.db import migrations, transaction
from django.utils import timezone


def populate_column(apps, schema_editor, model_name, column_name):
    search_model = apps.get_model("search", model_name)
    while (
        search_model.objects.exclude(**{f"{column_name}__isnull": True})
        .filter(**{f"{column_name}time__isnull": True})
        .exists()
    ):
        with transaction.atomic():
            for row in search_model.objects.filter(
                **{f"{column_name}time__isnull": True}
            )[:50_000]:
                _date = getattr(row, column_name)
                _datetime = (
                    timezone.make_aware(
                        datetime.combine(_date, time.min),
                        timezone.get_current_timezone(),
                    )
                    if _date
                    else None
                )
                setattr(row, f"{column_name}time", _datetime)
                row.save()


class Migration(migrations.Migration):
    dependencies = [
        ("search", "0059_add_datetimes_drop_views"),
    ]

    operations = [
        migrations.RunPython(
            partial(
                populate_column,
                model_name="TransactionSearch",
                column_name="last_modified_date",
            ),
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            partial(
                populate_column,
                model_name="TransactionSearch",
                column_name="initial_report_date",
            ),
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            partial(
                populate_column,
                model_name="AwardSearch",
                column_name="last_modified_date",
            ),
            migrations.RunPython.noop,
        ),
    ]
