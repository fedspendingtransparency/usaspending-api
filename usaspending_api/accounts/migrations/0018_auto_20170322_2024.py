# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0017_auto_20170320_1455'),
    ]

    tas_fk_sql = '''
        update treasury_appropriation_account t
        set federal_account_id = fa.id
        from federal_account fa
        where t.agency_id = fa.agency_identifier
        and t.main_account_code = fa.main_account_code
        '''

    def insert_federal_accounts(apps, schema_editor):
        TreasuryAppropriationAccount = apps.get_model(
            "accounts", "TreasuryAppropriationAccount")
        FederalAccount = apps.get_model("accounts", "FederalAccount")
        # Get a count of distinct combinations of agency id/main acct code
        # in the TAS table.
        expected_federal_accounts = TreasuryAppropriationAccount.objects.distinct(
            'main_account_code', 'agency_id').count()

        # Get TAS agency id (AID), main acct code (MAC), and the acct title for the most
        # recent period end date for each agency/acct code combo. These
        # will be the federal account records.
        federal_accounts = TreasuryAppropriationAccount.objects.values_list(
            'agency_id', 'main_account_code', 'account_title').distinct(
                'agency_id', 'main_account_code').order_by(
                'agency_id', 'main_account_code', '-ending_period_of_availability')
        fa_objects = [FederalAccount(
            agency_identifier=f[0],
            main_account_code=f[1],
            account_title=f[2]) for f in federal_accounts]
        FederalAccount.objects.bulk_create(fa_objects)

        # Make sure the distinct query we run above to get the federal acct
        # records returns the same number of rows as doing a straight up
        # distinct on AID/MAC. If not, abort the mission.
        if expected_federal_accounts != federal_accounts.count():
            raise ValueError('Federal account check failed: aborting migration')

    operations = [
        migrations.CreateModel(
            name='FederalAccount',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('agency_identifier', models.CharField(db_index=True, max_length=3)),
                ('main_account_code', models.CharField(db_index=True, max_length=4)),
                ('account_title', models.CharField(max_length=300)),
            ],
            options={
                'managed': True,
                'db_table': 'federal_account',
            },
        ),

        migrations.AlterUniqueTogether(
            name='federalaccount',
            unique_together=set([('agency_identifier', 'main_account_code')]),
        ),

        # insert records to newly-created finanical_accouns table
        migrations.RunPython(insert_federal_accounts, migrations.RunPython.noop),

        # add federal account foreign key to TAS table
        migrations.AddField(
            model_name='treasuryappropriationaccount',
            name='federal_account',
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.DO_NOTHING,
                to='accounts.FederalAccount'),
            preserve_default=False,
        ),

        # set values for new federal account FK
        migrations.RunSQL(tas_fk_sql, migrations.RunSQL.noop),
    ]
