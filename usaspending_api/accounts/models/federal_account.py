from django.db import models
from usaspending_api.common.exceptions import InvalidParameterException


class FederalAccount(models.Model):
    """
    Represents a single federal account. A federal account encompasses multiple Treasury Account Symbols (TAS),
    represented by: model:`accounts.TreasuryAppropriationAccount`.
    """

    agency_identifier = models.TextField(db_index=True)
    main_account_code = models.TextField(db_index=True)
    account_title = models.TextField()
    federal_account_code = models.TextField(null=True)  # agency_identifier + '-' + main_account_code
    parent_toptier_agency = models.ForeignKey(
        "references.ToptierAgency",
        models.DO_NOTHING,
        null=True,
        help_text=(
            "The toptier agency under which this federal account should appear in lists and dropdowns.  Not "
            "as simple as just mapping the AID to an agency, although AID does factor into the decision."
        ),
    )

    class Meta:
        managed = True
        db_table = "federal_account"
        unique_together = ("agency_identifier", "main_account_code")

    @staticmethod
    def fa_rendering_label_to_component_dictionary(fa_rendering_label) -> dict:
        try:
            components = fa_rendering_label.split("-")
            retval = {}

            retval["aid"] = components[0]
            retval["main"] = components[1]

            return retval
        except Exception:
            raise InvalidParameterException(
                f"Cannot parse provided Federal Account: {fa_rendering_label}. Valid examples: 000-0102, 015-8591"
            )
