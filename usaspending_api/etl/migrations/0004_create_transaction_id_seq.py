"""
Create the "transaction_id_seq" as a Postgres sequence used to keep track of ID values in
"transaction_id_lookup" for Delta.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("etl", "0003_duns_schema_change"),
    ]

    operations = [
        migrations.RunSQL(
            sql="CREATE SEQUENCE IF NOT EXISTS transaction_id_seq AS BIGINT",
            reverse_sql="DROP SEQUENCE IF EXISTS transaction_id_seq"
        ),

    ]
