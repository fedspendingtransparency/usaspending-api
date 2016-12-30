from collections import namedtuple

from rest_framework import serializers


class LimitableSerializer(serializers.ModelSerializer):
    """Extends the model serializer to support field limiting."""

    def get_fields(self):
        """Override serializer's get_fields method to support dynamic field limiting."""
        fields = super(LimitableSerializer, self).get_fields()
        exclude_fields, include_fields, model_table = self.get_exclude_include_fields()

        # We must exclude before include to avoid conflicts
        for exclude in exclude_fields:
            if exclude.field in fields:
                fields.pop(exclude.field)
            elif exclude.field == model_table:
                fields.clear()

        include_fieldnames = [i.field for i in include_fields]
        if len(include_fields) == 0 or model_table in include_fieldnames:
            # Either the request doesn't include the fields parameter, or the
            # field parameter is passing a model object name (versus an
            # individual field name0. For both scenarios, we want all fields
            # (except for any that have already been excluded) to be included
            # in the response.
            allowed = set(fields.keys())
        else:
            allowed = set(include_fieldnames)
        existing = set(fields.keys())
        for field_name in existing - allowed:
            fields.pop(field_name)

        return fields

    def map_nested_serializer(self, field_name, nested_serializer):
        """
        Assign a nested serializer to a field in a parent serializer (if
        appropriate) based on request field and exclude parameters.
        This function is called during the parent serializer's __init__,
        which enables context to be passed to nested/child serializers.
        """
        exclude_fields, include_fields, nested_model_table = self.get_exclude_include_fields()

        # Attach the specified serializer to the specified field, if
        # appropriation. The purpose of this code is to ensure that the
        # response doesn't serialize nested objects when it doesn't make
        # sense to do so. For example, if a request is limiting response
        # data to fields not in the nested serializer, we don't want to
        # instantiate that nested serializer at all. Also, if a request
        # excludes the nested serializer's entire model, we don't want to
        # instantiate the nested serializer. This ensures that the response
        # doesn't include empty nested objects.
        if nested_model_table in [exclude.field for exclude in exclude_fields]:
            return
        if len(include_fields) == 0:
            # Request is not using the fields parameter, so instantiate the
            # nested serializer and and attach it to its corresonding field
            self.fields[field_name] = nested_serializer(
                read_only=True, context=self.context)

        self.fields[field_name] = nested_serializer(
            read_only=True, context=self.context)

    def get_exclude_include_fields(self):
        # get the name of the serializer's underlying table
        if 'model_table' in self.context:
            # Many=True nested serializers are limitable, not model-based
            # Thus, there is no Meta, and we have to pass the underlying
            # table name as part of the context
            serializer_model_table = self.context['model_table']
        else:
            serializer_model_table = self.Meta.model._meta.db_table

        RequestField = namedtuple('RequestField', ['model', 'field'])
        request = self.context.get('request')
        if request:
            # Workaround to use this serializer with both GET and POST
            params = request.query_params.copy()
            params.update(request.data.copy())
            exclude_fields = params.get('exclude', [])
            include_fields = params.get('fields', [])
        else:
            exclude_fields, include_fields = [], []

        # Field and exclude parameters for nested fields will be coming in as
        # standard Django FK syntax (thus the split('__') command to parse
        # out field names and model table names
        exclude_fields = [RequestField(f.split('__')[:-1], f.split('__')[-1]) for f in exclude_fields]
        include_fields = [RequestField(f.split('__')[:-1], f.split('__')[-1]) for f in include_fields]

        return exclude_fields, include_fields, serializer_model_table


class AggregateSerializer(serializers.Serializer):

    item = serializers.CharField(required=False)
    aggregate = serializers.DecimalField(20, 2)
