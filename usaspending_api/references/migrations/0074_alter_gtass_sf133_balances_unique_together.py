from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("references", "0073_gtassf133balances_bea_category_and_more"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="gtassf133balances",
            unique_together={
                (
                    "fiscal_year",
                    "fiscal_period",
                    "disaster_emergency_fund",
                    "tas_rendering_label",
                    "budget_object_class",
                    "program_activity_reporting_key",
                    "prior_year_adjustment",
                    "by_direct_reimbursable_fun",
                    "bea_category",
                )
            },
        ),
    ]
