"""
This migration was manually created to shorten the name of index names. This is not a Postgres requirement,
but it is a Django requirement for the Index object. This was previously not an issue because we leveraged a different
library for the creation of these partial indexes.
"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("recipient", "0022_delete_summaryawardrecipient"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER INDEX recipient_l_duns_a43c07_partial RENAME TO rl_duns_a43c07_partial",
            reverse_sql="ALTER INDEX rl_duns_a43c07_partial RENAME TO recipient_l_duns_a43c07_partial",
            state_operations=[
                migrations.AddIndex(
                    model_name="recipientlookup",
                    index=models.Index(name="rl_duns_a43c07_partial", fields=["duns"], condition=models.Q(duns__isnull=False))
                ),
            ]
        ),
        migrations.RunSQL(
            sql="ALTER INDEX recipient_l_parent__efd6d5_partial RENAME TO rl_parent__efd6d5_partial",
            reverse_sql="ALTER INDEX rl_parent__efd6d5_partial RENAME TO recipient_l_parent__efd6d5_partial",
            state_operations=[
                migrations.AddIndex(
                    model_name="recipientlookup",
                    index=models.Index(name="rl_parent__efd6d5_partial", fields=["parent_duns"], condition=models.Q(parent_duns__isnull=False))
                ),
            ]
        ),
        migrations.RunSQL(
            sql="ALTER INDEX recipient_l_parent__271f5c_partial RENAME TO rl_parent__271f5c_partial",
            reverse_sql="ALTER INDEX rl_parent__271f5c_partial RENAME TO recipient_l_parent__271f5c_partial",
            state_operations=[
                migrations.AddIndex(
                    model_name="recipientlookup",
                    index=models.Index(name="rl_parent__271f5c_partial", fields=["parent_uei"], condition=models.Q(parent_uei__isnull=False))
                ),
            ]
        ),

    ]
