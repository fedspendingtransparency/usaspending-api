from django.db import models


class FrecMap(models.Model):
    """Used to find FR_entity_code for load_budget_authority"""

    id = models.AutoField(primary_key=True)
    agency_identifier = models.TextField(null=False, db_index=True)  # source: AID
    main_account_code = models.TextField(null=False, db_index=True)  # source: MAIN
    treasury_appropriation_account_title = models.TextField(null=False, db_index=True)  # source: GWA_TAS_NAME
    sub_function_code = models.TextField(null=False, db_index=True)  # source: Sub Function Code; dest: Subfunction Code
    fr_entity_code = models.TextField(null=False)  # source: FR Entity Type

    class Meta:
        db_table = "frec_map"
