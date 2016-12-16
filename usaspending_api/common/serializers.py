from rest_framework import serializers


# Extends model serializer to support limiting fields via kwargs
class LimitableSerializer(serializers.ModelSerializer):
    def __init__(self, *args, **kwargs):
        override_fields = kwargs.pop('fields', None)
        exclude_fields = kwargs.pop('exclude', None)
        super(LimitableSerializer, self).__init__(*args, **kwargs)

        # We must exclude before include to avoid conflicts from user error
        if exclude_fields:
            for field_name in exclude_fields:
                self.fields.pop(field_name)

        if override_fields:
            allowed = set(override_fields)
            existing = set(self.fields.keys())
            for field_name in existing - allowed:
                self.fields.pop(field_name)


class AggregateSerializer(serializers.Serializer):

    # todo: probably don't return a datetime object as a string
    item = serializers.CharField()
    aggregate = serializers.DecimalField(20, 2)
