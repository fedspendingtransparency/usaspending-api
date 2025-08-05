from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('search', '0039_transactionsearch_initial_report_date'),
    ]

    # You may notice that the unmanaged models here have no fields.  This is an intentional
    # attempt to cut back a bit on unnecessary migration code.  Unmanaged models do not require
    # fields unless they are referenced by another model so this should cause no harm.
    operations = [
        migrations.CreateModel(
            name='AgencyOfficeAutocompleteMatview',
            fields=[],
            options={
                'db_table': 'mv_agency_office_autocomplete',
                'managed': False,
            },
        ),
    ]
