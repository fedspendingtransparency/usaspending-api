"""
Create the "award_id_seq" as a Postgres sequence used to keep track of ID values in
"award_id_lookup" for Delta.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("etl", "0004_create_transaction_id_seq"),
    ]

    operations = [
        migrations.RunSQL(
            sql="CREATE SEQUENCE IF NOT EXISTS award_id_seq AS BIGINT",
            reverse_sql="DROP SEQUENCE IF EXISTS award_id_seq"
        ),

    ]
