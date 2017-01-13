from rest_framework import serializers


class LimitableSerializer(serializers.ModelSerializer):
    """Extends the model serializer to support field limiting."""
    def __init__(self, *args, **kwargs):
        # next two lines are deprecated and will be removed
        # once all views inherit from a generic class or are
        # refactored into viewsets (i.e., once the serializer
        # consistently has direct access to the request)
        include_fields = kwargs.pop('fields', None)
        exclude_fields = kwargs.pop('exclude', None)
        super(LimitableSerializer, self).__init__(*args, **kwargs)

        # Create and initialize the child serializers
        try:
            # Initialize the child serializers
            children = self.Meta.nested_serializers
            for field in children.keys():
                child_args = {
                    **children[field].get("kwargs", {}),
                    "context": self.context
                }
                self.fields[field] = children[field]["class"](**child_args)
        except:
            # We don't have any nested serializers
            pass

        request = self.context.get('request')
        if request:
            # workaround to use this serializer with both GET and POST
            params = dict(request.query_params)
            params.update(dict(request.data))
            exclude_fields = params.get('exclude')
            include_fields = params.get('fields')

            if params.get('verbose', False):
                # We have a request for verbose, so we return here so that we
                # return all fields
                return

        # We must exclude before include to avoid conflicts from user error
        if exclude_fields is not None:
            for field_name in exclude_fields:
                self.fields.pop(field_name)

        if include_fields is not None:
            allowed = set(include_fields)
            existing = set(self.fields.keys())
            for field_name in existing - allowed:
                self.fields.pop(field_name)
        else:
            try:
                include_fields = self.Meta.model.get_default_fields()
                allowed = set(include_fields)
                existing = set(self.fields.keys())
                for field_name in existing - allowed:
                    self.fields.pop(field_name)
            except Exception as e:
                pass


class AggregateSerializer(serializers.Serializer):

    item = serializers.CharField(required=False)
    aggregate = serializers.DecimalField(20, 2)
