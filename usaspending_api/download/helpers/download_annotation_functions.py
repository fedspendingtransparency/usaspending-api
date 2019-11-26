from django.contrib.postgres.aggregates import StringAgg
from usaspending_api.common.helpers.orm_helpers import FiscalYear
from django.db.models.functions import Concat
from django.db.models import Value


def universal_transaction_matview_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": StringAgg(
            "transaction__award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "transaction__award__financial_set__treasury_account__federal_account__federal_account_code",
            ";",
            distinct=True,
        ),
    }
    return annotation_fields


def universal_award_matview_annotations():
    annotation_fields = {
        "treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "usaspending_permalink": Concat(Value("https://usaspending.gov/#/award/"), "award__generated_unique_award_id"),
    }
    return annotation_fields


def idv_order_annotations():
    annotation_fields = {
        "treasury_accounts_funding_this_award": StringAgg(
            "financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "usaspending_permalink": Concat(Value("https://usaspending.gov/#/award/"), "generated_unique_award_id"),
    }
    return annotation_fields


def idv_transaction_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
    }
    return annotation_fields


def subaward_annotations():
    annotation_fields = {
        "subaward_action_date_fiscal_year": FiscalYear("subaward__action_date"),
        "prime_award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "prime_award_federal_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
        ),
        "prime_award_treasury_accounts_funding_this_award": StringAgg(
            "award__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
        ),
        "usaspending_permalink": Concat(Value("https://usaspending.gov/#/award/"), "award__generated_unique_award_id"),
    }
    return annotation_fields
