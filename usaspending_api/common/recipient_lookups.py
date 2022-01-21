from django.db.models import CharField, Expression
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES


def obtain_recipient_uri(
    recipient_name, recipient_unique_id, recipient_uei, parent_recipient_unique_id, is_parent_recipient=False
):
    """Return a valid string to be used for api/v2/recipient/duns/<recipient-hash>/ (or None)

    Keyword Arguments:
    recipient_name -- Legal Entity Name from the record
    recipient_unique_id -- DUNS from the record
    parent_recipient_unique_id -- parent DUNS from the record
    is_parent_recipient -- boolean flag to force the recipient level to be "P" (default False)
        By the nature of transaction records, the listed recipient can only be "R" or "C"
        This flag is for the parent recipient link (as appropriate)

    Return example string: 11fcdf15-3490-cdad-3df4-3b410f3d9b20-C

    """
    if (is_parent_recipient and not recipient_unique_id) or not (recipient_unique_id or recipient_name):
        return None

    if recipient_unique_id:
        recipient_hash = fetch_recipient_hash_using_duns(recipient_unique_id)
    else:
        recipient_hash = None

    if recipient_hash is None:
        recipient_hash = generate_missing_recipient_hash(recipient_unique_id, recipient_uei, recipient_name)

    recipient_level = obtain_recipient_level(
        {
            "duns": recipient_unique_id,
            "parent_duns": parent_recipient_unique_id,
            "uei": recipient_uei,
            "is_parent_recipient": is_parent_recipient,
        }
    )

    # Confirm that a recipient profile exists for the recipient information we have collected/generated.
    if RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level=recipient_level).exists():
        return combine_recipient_hash_and_level(recipient_hash, recipient_level)

    return None


def generate_missing_recipient_hash(recipient_unique_id, recipient_uei, recipient_name):
    import hashlib
    import uuid

    if recipient_unique_id is None and recipient_uei is None:
        prefix = "name"
        value = recipient_name
    elif recipient_uei is None:
        prefix = "duns"
        value = recipient_unique_id
    else:
        prefix = "uei"
        value = recipient_uei

    return str(uuid.UUID(hashlib.md5(f"{prefix}-{value}".upper().encode("utf-8")).hexdigest()))


def fetch_recipient_hash_using_duns(recipient_unique_id):
    recipient = RecipientLookup.objects.filter(duns=recipient_unique_id).values("recipient_hash").first()

    return recipient["recipient_hash"] if recipient else None


def obtain_recipient_level(recipient_record: dict) -> str:
    level = None
    if recipient_is_parent(recipient_record):
        level = "P"
    elif recipient_is_standalone(recipient_record):
        level = "R"
    elif recipient_is_child(recipient_record):
        level = "C"
    return level


def recipient_is_parent(recipient_record: dict) -> bool:
    return recipient_record["is_parent_recipient"]


def recipient_is_standalone(recipient_record: dict) -> bool:
    return recipient_record["parent_duns"] is None


def recipient_is_child(recipient_record: dict) -> bool:
    return recipient_record["parent_duns"] is not None


def combine_recipient_hash_and_level(recipient_hash, recipient_level):
    return f"{recipient_hash}-{recipient_level.upper()}"


def _annotate_recipient_id(field_name, queryset, annotation_sql):
    """
    Add recipient id (recipient hash + recipient level) to a queryset.  The assumption here is that
    the queryset is based on a data source that contains recipient_unique_id and
    parent_recipient_unique_id which, currently, all of our advanced search materialized views do.
    """

    class RecipientId(Expression):
        """
        Used to graft a subquery into a queryset that can build recipient ids.

        This is a bit less than ideal, but I just couldn't construct an ORM query to mimic this
        logic.  There are several issues including but not limited to:

            - There are currently no relations between these tables in the Django ORM which makes
              joining them... challenging.
            - Adding relations to the ORM changes how the fields behave making this a much bigger
              enhancement than originally planned.
            - When I did add relations to the ORM, I couldn't figure out how to make the Django
              OuterRef expression check for nulls since the subquery needs to check to see if the
              parent_recipient_unique_id in the outer query is null.

        Anyhow, this works and is encapsulated so if someone smart figures out how to use pure ORM,
        it should be easy to patch in.
        """

        def __init__(self):
            super(RecipientId, self).__init__(CharField())

        def as_sql(self, compiler, connection):
            return (
                convert_composable_query_to_string(
                    SQL(annotation_sql).format(
                        outer_table=Identifier(compiler.query.model._meta.db_table),
                        special_cases=Literal(tuple(sc for sc in SPECIAL_CASES)),
                    )
                ),
                [],
            )

    return queryset.annotate(**{field_name: RecipientId()})


def annotate_recipient_id(field_name, queryset):
    return _annotate_recipient_id(
        field_name,
        queryset,
        """(
            select
                rp.recipient_hash || '-' ||  rp.recipient_level
            from
                recipient_profile rp
                inner join recipient_lookup rl on rl.recipient_hash = rp.recipient_hash
            where
                (
                    (
                        {outer_table}.recipient_unique_id is null
                        and rl.duns is null
                        and {outer_table}.recipient_name = rl.legal_business_name
                    ) or (
                        {outer_table}.recipient_unique_id is not null
                        and rl.duns is not null
                        and rl.duns = {outer_table}.recipient_unique_id
                    )
                )
                and rp.recipient_level = case
                    when {outer_table}.parent_recipient_unique_id is null then 'R'
                    else 'C' end
                and rp.recipient_name not in {special_cases}
        )""",
    )


def annotate_prime_award_recipient_id(field_name, queryset):
    return _annotate_recipient_id(
        field_name,
        queryset,
        """(
            select
                rp.recipient_hash || '-' ||  rp.recipient_level
            from
                broker_subaward bs
                inner join recipient_lookup rl on rl.duns = bs.awardee_or_recipient_uniqu
                inner join recipient_profile rp on rp.recipient_hash = rl.recipient_hash
            where
                bs.id = {outer_table}.subaward_id and
                rp.recipient_level = case
                    when bs.ultimate_parent_unique_ide is null or bs.ultimate_parent_unique_ide = '' then 'R'
                    else 'C'
                end and
                rp.recipient_name not in {special_cases}
        )""",
    )
