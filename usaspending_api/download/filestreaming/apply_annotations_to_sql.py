import re


NAMING_CONFLICT_SUFFIX = "_NAMING_CONFLICT_SUFFIX"


def apply_annotations_to_sql(raw_query, aliases):
    """
    Postgres offers the ability to SELECT raw SQL directly to a file (CSV, TSV, etc) using psql from the command
    line which is much more efficient than returning the results to Python and saving those results to a file.
    To this end, we need explicit control over SELECT aliases and the order in which SELECT values are returned
    in order to meet file structure requirements.

    Django's ORM does not allow SELECT column/annotation aliases to conflict with the underlying model field
    names.  Additionally, Django's ORM does not allow us to specify the order in which SELECT columns/annotations
    are returned (annotations are always last).

    This function rectifies these issues by dissecting the raw SQL and stitching it back together with the desired
    aliases (which may or may not conflict with underlying model field names) and in the desired output order.
    """
    aliases_copy = list(aliases)

    # Extract everything between the first SELECT and the last FROM
    first_query_in_union = raw_query.split(" UNION ")[0]
    query_before_group_by = first_query_in_union.split("GROUP BY ")[0]
    query_before_from = re.sub(r"\(?SELECT ", "", " FROM".join(re.split(" FROM", query_before_group_by)[:-1]), count=1)

    # Create a list from the non-derived values between SELECT and FROM
    selects_str = re.findall(
        r"SELECT (.*?) (CASE|CONCAT|SUM|COALESCE|STRING_AGG|MAX|EXTRACT|\(SELECT|FROM)", raw_query
    )[0]
    just_selects = selects_str[0] if selects_str[1] == "FROM" else selects_str[0][:-1]
    selects_list = [select.strip() for select in just_selects.strip().split(",")]

    # Create a list from the derived values between SELECT and FROM
    remove_selects = query_before_from.replace(selects_str[0], "")
    deriv_str_lookup = re.findall(
        r"(CASE|CONCAT|SUM|COALESCE|STRING_AGG|MAX|EXTRACT|\(SELECT|)(.*?) AS (.*?)( |$)", remove_selects
    )
    deriv_dict = {}
    for str_match in deriv_str_lookup:
        # Remove trailing comma and surrounding quotes from the alias, add to dict, remove from alias list
        alias = str_match[2][:-1].strip() if str_match[2][-1:] == "," else str_match[2].strip()
        if (alias[-1:] == '"' and alias[:1] == '"') or (alias[-1:] == "'" and alias[:1] == "'"):
            alias = alias[1:-1]
        deriv_dict[alias] = "{}{}".format(str_match[0], str_match[1]).strip()
        # Provides some safety if a field isn't provided in this historical_lookup
        if alias in aliases_copy:
            aliases_copy.remove(alias)

    # Validate we have an alias for each value in the SELECT string
    if len(selects_list) != len(aliases_copy):
        raise Exception(
            f"Length of aliases ({len(aliases_copy)}) doesn't match the columns in selects ({len(selects_list)})"
        )

    # Match aliases with their values
    values_list = [
        f'{deriv_dict[alias] if alias in deriv_dict else selects_list.pop(0)} AS "{alias}"' for alias in aliases
    ]

    # For each query in a UNION, apply our SELECT statement "fixes".  This, of course, requires that the SELECT
    # statement for each query in the UNION to be identical which will probably fail some day, but works for our
    # current use cases.
    sql = " UNION ".join([s.replace(query_before_from, ", ".join(values_list), 1) for s in raw_query.split(" UNION ")])

    # Now that we've converted the queryset to SQL, cleaned up aliasing for non-annotated fields, and sorted
    # the SELECT columns, there's one final step.  The Django ORM does now allow alias names to conflict with
    # column/field names on the underlying model.  For annotated fields, naming conflict exceptions occur at
    # the time they are applied to the queryset which means they never get to this function.  To work around
    # this, we give them a temporary name that cannot conflict with a field name on the model by appending
    # the suffix specified by NAMING_CONFLICT_SUFFIX.  Now that we have the "final" SQL, we must remove that suffix.
    return sql.replace(NAMING_CONFLICT_SUFFIX, "")
