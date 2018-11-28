def drop_like_pattern_ops_index(app_label, model_name, column_name):
    """
    The goal here is to drop a PostgreSQL "like" (pattern_ops_index) index for
    a column/field from within a migration.  Django creates an index of this
    type every time it indexes a text/varchar field in PostgreSQL, however, if
    we're not using it, why keep it?  It just wastes space and increases
    maintenance overhead.  Since we can't seem to prevent the creation of these
    indexes on a primary key, our next best bet if we don't want to roll our
    own primary key creation code is to drop it right after creation.  Dumb,
    but effective.  For normal non-primary-key indexes, we can bypass creation
    of a "like" index by using the model._meta.indexes option in Django 1.11 or
    higher.

    app_label is the name of the Django application containing the model in
    question.  For example, to drop a "like" index on the DUNS table, we would
    want to provide 'recipient' here.

    model_name is the name of the Django model from which we wish to drop a
    "like" index.  For example, to alter the DUNS table, we would want to
    provide 'DUNS' here.

    column_name is the name of the model column upon which the "like" index
    was built.  For example, to remove the "like" index from the
    'awardee_or_recipient_uniqu' column in the DUNS table, we would provide
    'awardee_or_recipient_uniqu' here.

    It is ok if a "like" index does not exist on the column indicated.  An
    exception will not be thrown.

    Bits of this function were modeled after Django functions found in
    site-packages/django/db/backends/base/schema.py, primarily
    _delete_composed_index.  Yes, in
    site-packages/django/db/backends/postgresql/schema.py '_like' is
    hardcoded everywhere as the suffix for these types of indexes so we will
    follow suit.
    """
    def _drop_like_pattern_ops_index(apps, schema_editor):
        model = apps.get_model(app_label, model_name)
        column = model._meta.get_field(column_name).column
        constraint_names = [
            c for c in schema_editor._constraint_names(model, [column]) if c.endswith('_like')
        ]
        if constraint_names:
            for constraint_name in constraint_names:
                print('Dropping index {}'.format(constraint_name))
                schema_editor.execute(
                    schema_editor._delete_constraint_sql(
                        schema_editor.sql_delete_index, model, constraint_name
                    )
                )
        else:
            print('No "like" indexes found for %s.%s.%s' % (app_label, model_name, column_name))
    return _drop_like_pattern_ops_index
