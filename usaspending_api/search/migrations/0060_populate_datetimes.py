from datetime import datetime, time

from django.apps.registry import Apps
from django.db import migrations, transaction
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.utils import timezone


def populate_transaction_search(
    apps: Apps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    TransactionSearch = apps.get_model("search", "TransactionSearch")
    while (
        TransactionSearch.objects.exclude(
            last_modified_date__isnull=True, initial_report_date__isnull=True
        )
        .filter(
            last_modified_datetime__isnull=True, initial_report_datetime__isnull=True
        )
        .exists()
    ):
        with transaction.atomic():
            for row in TransactionSearch.objects.filter(
                last_modified_datetime__isnull=True,
                initial_report_datetime__isnull=True,
            ).values("transaction_id", "last_modified_date", "initial_report_date")[
                :50_000
            ]:
                row["last_modified_datetime"] = (
                    timezone.make_aware(
                        datetime.combine(row["last_modified_date"], time.min),
                        timezone.get_current_timezone(),
                    )
                    if row["last_modified_date"]
                    else None
                )
                row["initial_report_datetime"] = (
                    timezone.make_aware(
                        datetime.combine(row["initial_report_date"], time.min),
                        timezone.get_current_timezone(),
                    )
                    if row["initial_report_date"]
                    else None
                )
                TransactionSearch(**row).save(
                    update_fields=["last_modified_datetime", "initial_report_datetime"]
                )


def populate_award_search(apps: Apps, schema_editor: BaseDatabaseSchemaEditor) -> None:
    AwardSearch = apps.get_model("search", "AwardSearch")
    while (
        AwardSearch.objects.exclude(last_modified_date__isnull=True)
        .filter(last_modified_datetime__isnull=True)
        .exists()
    ):
        with transaction.atomic():
            for row in AwardSearch.objects.filter(
                last_modified_datetime__isnull=True
            ).values("award_id", "last_modified_date")[:50_000]:
                row["last_modified_datetime"] = (
                    timezone.make_aware(
                        datetime.combine(row["last_modified_date"], time.min),
                        timezone.get_current_timezone(),
                    )
                    if row["last_modified_date"]
                    else None
                )

                AwardSearch(**row).save(update_fields=["last_modified_datetime"])


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("search", "0059_add_datetimes_drop_views"),
    ]

    operations = [
        migrations.RunPython(
            populate_transaction_search,
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            populate_award_search,
            migrations.RunPython.noop,
        ),
    ]
