from django.db import models


class HistoricalAppropriationAccountBalances(models.Model):
    """
    This table is a work in progress to support new Agency 2.0 functionality on the USAspending
    website.  It is based on a one time dump of GTAS File A data and contains data similar to, but
    not quite the same as, data in the AppropriationAccountBalances and TreasuryAppropriationAccount
    tables which are loaded from a regular GTAS feed (via Broker).

    The decision was made to keep this data separate for now for reasons including, but not
    limited to:

        - The data points we have received are ever so slightly different than those received from
          our regular feed.
        - This table will likely require several tweaks/refactors/reloads as we receive new dumps
          during the feature development cycle.
        - Combining this with AppropriationAccountBalances would risk desynchronizing us from Broker
          which would be bad.

    NOTE:  If/when we decide to keep this table, we will want to add another step to the
    load_reference_data management command to bootstrap values into this table.  I am intentionally
    skipping this step for now as this is a transitional table that is not currently being used and
    the source data for which will be changing.  It is being added to assist with feature development
    and to provide a jumping off point for future loads; no sense re-inventing the wheel each time
    we get a new load.
    """

    historical_appropriation_account_balances_id = models.AutoField(primary_key=True)

    tas_rendering_label = models.TextField(blank=True, null=True)

    allocation_transfer_agency_id = models.TextField(blank=True, null=True)
    agency_id = models.TextField()
    beginning_period_of_availability = models.TextField(blank=True, null=True)
    ending_period_of_availability = models.TextField(blank=True, null=True)
    availability_type_code = models.TextField(blank=True, null=True)
    main_account_code = models.TextField()
    sub_account_code = models.TextField()
    account_title = models.TextField()

    budget_function_code = models.TextField(blank=True, null=True)
    budget_subfunction_code = models.TextField(blank=True, null=True)

    fr_entity_code = models.TextField(blank=True, null=True)

    total_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)

    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    reporting_fiscal_year = models.IntegerField(blank=True, null=True)
    reporting_fiscal_quarter = models.IntegerField(blank=True, null=True)
    reporting_fiscal_period = models.IntegerField(blank=True, null=True)

    owning_toptier_agency = models.ForeignKey("references.ToptierAgency", models.DO_NOTHING, null=True)

    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        db_table = "historical_appropriation_account_balances"
