from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient_duns import RecipientDunsViewSet


class RecipientViewSet(RecipientDunsViewSet):
    """
    This route takes award filters and returns spending by Recipient UEI or DUNS.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/recipient.md"

