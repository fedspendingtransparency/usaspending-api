from rest_framework import serializers
import re


# Extends model serializer to support limiting fields via kwargs
class LimitableSerializer(serializers.ModelSerializer):
    def __init__(self, *args, **kwargs):
        override_fields = kwargs.pop('fields', None)
        exclude_fields = kwargs.pop('exclude', None)
        self.include_subfields = {}
        self.exclude_subfields = {}
        super(LimitableSerializer, self).__init__(*args, **kwargs)

        # Store any sub-serializer fields
        if override_fields:
            for field in list(override_fields):  # We run over a duplicate of this list because we may modify it
                if self.check_and_store_subfield(field, self.include_subfields, append_target=override_fields):
                    override_fields.remove(field)

        if exclude_fields:
            for field in list(exclude_fields):  # Run over a duplicate because we may modify the list
                if self.check_and_store_subfield(field, self.exclude_subfields):
                    exclude_fields.remove(field)

        # We must exclude before include to avoid conflicts from user error
        if exclude_fields:
            for field_name in exclude_fields:
                self.fields.pop(field_name)

        # Remove all fields except those permitted
        if override_fields:
            allowed = set(override_fields)
            existing = set(self.fields.keys())
            for field_name in existing - allowed:
                self.fields.pop(field_name)

    def check_and_store_subfield(self, field_name, subfield_dict, append_target=None):
        # Looks for the first occurance of __, i.e. a FK traversal
        # Gives back group1 as the child name and group2 as the child's field
        # i.e. recipient__location__location_state_code would return
        # group1: recipient and group2: location__location_state_code
        # (this will cascade down)
        pattern = "^(.*?)__(.*)"
        matches = re.match(pattern, field_name)
        if matches:
            subserializer = matches.group(1)
            subfield = matches.group(2)

            if subserializer in subfield_dict:
                subfield_dict[subserializer].append(subfield)
            else:
                subfield_dict[subserializer] = [subfield]

            # Let's say a user passes in awarding_agency__cgac_code but did not
            # also include awarding_agency in their included fields. This would
            # mean that they wouldn't get back the cgac_code like they expect.
            # So, we add the subserializer (awarding_agency) to the field list
            # if it does not exist in there and they are looking for a subfield
            # of that subserializer
            if append_target and subserializer not in append_target:
                append_target.append(subserializer)

            return True
        else:
            return False

    # Used by implemented serializers to generate subserializers with cascading field limiting
    def get_subserializer_data(self, serializer_class, serializer_data, field_name, **kwargs):
        return serializer_class(serializer_data,
                                fields=self.include_subfields.get(field_name, None),
                                exclude=self.exclude_subfields.get(field_name, None),
                                **kwargs).data
