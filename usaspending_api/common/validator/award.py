"""
Some shortcuts for generating "standardized" basic award id TinyShield models.
These functions return your own personal copy of the TinyShield model so no
need to deepcopy!  Unless you're incorporating them into another model...
"""


def get_generated_award_id_model(key="award_id", name="award_id", optional=False):
    return {"key": key, "name": name, "type": "text", "text_type": "search", "optional": optional}


def get_internal_award_id_model(key="award_id", name="award_id", optional=False):
    return {"key": key, "name": name, "type": "integer", "optional": optional}


def get_internal_or_generated_award_id_model(key="award_id", name="award_id", optional=False):
    return {
        "key": key,
        "name": name,
        "type": "any",
        "optional": optional,
        "models": [{"type": "integer"}, {"type": "text", "text_type": "search"}],
    }
