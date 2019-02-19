from usaspending_api.recipient.models import RecipientLookup


def obtain_recipient_uri(recipient_name, recipient_unique_id, parent_recipient_unique_id=None):
    if not recipient_unique_id:
        return create_recipient_uri_without_duns(recipient_name, recipient_unique_id, parent_recipient_unique_id)
    return fetch_recipient_uri_with_duns(recipient_unique_id, parent_recipient_unique_id)


def create_recipient_uri_without_duns(recipient_name, recipient_unique_id, parent_recipient_unique_id=None):
    recipient_hash = generate_missing_recipient_hash(recipient_name, recipient_unique_id)
    recipient_level = obtain_recipient_level({"duns": recipient_unique_id, "parent_duns": parent_recipient_unique_id})
    return combine_recipient_hash_and_level(recipient_hash, recipient_level)


def fetch_recipient_uri_with_duns(recipient_unique_id, parent_recipient_unique_id):
    recipient_hash = fetch_recipient_hash_using_duns(recipient_unique_id)
    recipient_level = obtain_recipient_level({"duns": recipient_unique_id, "parent_duns": parent_recipient_unique_id})
    return combine_recipient_hash_and_level(recipient_hash, recipient_level)


def generate_missing_recipient_hash(recipient_name, recipient_unique_id):
    # SQL: MD5(UPPER(CONCAT(awardee_or_recipient_uniqu, legal_business_name)))::uuid
    import hashlib
    import uuid

    h = hashlib.md5("{}{}".format(recipient_unique_id, recipient_name).upper().encode("utf-8")).hexdigest()
    return str(uuid.UUID(h))


def fetch_recipient_hash_using_duns(recipient_unique_id):
    recipient = (
        RecipientLookup.objects.filter(duns=recipient_unique_id)
        .values("recipient_hash")
        .first()
    )

    return recipient["recipient_hash"] if recipient else None


def obtain_recipient_level(recipient_record: dict) -> str:
    level = None
    if recipient_is_standalone(recipient_record):
        level = "R"
    elif recipient_is_child(recipient_record):
        level = "C"
    # Can never be associated with a "parent" recipient profile level
    return level


def recipient_is_standalone(recipient_record: dict) -> bool:
    return recipient_record["parent_duns"] is None


def recipient_is_child(recipient_record: dict) -> bool:
    return recipient_record["parent_duns"] is not None


def combine_recipient_hash_and_level(recipient_hash, recipient_level):
    return "{}-{}".format(recipient_hash, recipient_level.upper())
