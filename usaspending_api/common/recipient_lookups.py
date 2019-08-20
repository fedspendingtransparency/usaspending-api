from usaspending_api.recipient.models import RecipientLookup, RecipientProfile


def obtain_recipient_uri(recipient_name, recipient_unique_id, parent_recipient_unique_id, is_parent_recipient=False):
    """ Return a valid string to be used for api/v2/recipient/duns/<recipient-hash>/ (or None)

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
        recipient_hash = generate_missing_recipient_hash(recipient_unique_id, recipient_name)

    recipient_level = obtain_recipient_level(
        {
            "duns": recipient_unique_id,
            "parent_duns": parent_recipient_unique_id,
            "is_parent_recipient": is_parent_recipient,
        }
    )

    # Confirm that a recipient profile exists for the recipient information we have collected/generated.
    if RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level=recipient_level).exists():
        return combine_recipient_hash_and_level(recipient_hash, recipient_level)

    return None


def generate_missing_recipient_hash(recipient_unique_id, recipient_name):
    # SQL: MD5(UPPER(CONCAT(awardee_or_recipient_uniqu, legal_business_name)))::uuid
    import hashlib
    import uuid

    h = hashlib.md5("{}{}".format(recipient_unique_id, recipient_name).upper().encode("utf-8")).hexdigest()
    return str(uuid.UUID(h))


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
    return "{}-{}".format(recipient_hash, recipient_level.upper())
