from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Concat
from django.db.models import Value


def universal_transaction_matview_annotations():
    federal_account_query_path = "transaction__award__financial_set__treasury_account__federal_account"
    annotation_fields = {
        "federal_accounts_funding_this_award": StringAgg(
            Concat(
                "{}__agency_identifier".format(federal_account_query_path),
                Value("-"),
                "{}__main_account_code".format(federal_account_query_path),
            ),
            ";",
            distinct=True,
        )
    }
    return annotation_fields


def universal_award_matview_annotations():
    federal_account_query_path = "award__financial_set__treasury_account__federal_account"
    annotation_fields = {
        "federal_accounts_funding_this_award": StringAgg(
            Concat(
                "{}__agency_identifier".format(federal_account_query_path),
                Value("-"),
                "{}__main_account_code".format(federal_account_query_path),
            ),
            ";",
            distinct=True,
        )
    }
    return annotation_fields
