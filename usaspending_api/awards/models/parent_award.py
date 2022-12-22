from django.db import models


class ParentAward(models.Model):
    award = models.OneToOneField("search.AwardSearch", on_delete=models.CASCADE, primary_key=True)
    generated_unique_award_id = models.TextField(unique=True)
    parent_award = models.ForeignKey("self", on_delete=models.CASCADE, db_index=True, blank=True, null=True)

    direct_idv_count = models.IntegerField()
    direct_contract_count = models.IntegerField()
    direct_total_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    direct_base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2)
    direct_base_exercised_options_val = models.DecimalField(max_digits=23, decimal_places=2)

    rollup_idv_count = models.IntegerField()
    rollup_contract_count = models.IntegerField()
    rollup_total_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    rollup_base_and_all_options_value = models.DecimalField(max_digits=23, decimal_places=2)
    rollup_base_exercised_options_val = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        managed = True
        db_table = "parent_award"
