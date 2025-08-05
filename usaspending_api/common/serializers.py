from rest_framework import serializers


class LimitableSerializer(serializers.ModelSerializer):
    prefetchable = True

    """Extends the model serializer to support field limiting."""

    def __init__(self, *args, **kwargs):
        # Grab any kwargs include and exclude fields, these are typically
        # passed in by a parent serializer to the child serializer
        kwargs_has_include = "fields" in kwargs
        kwargs_has_exclude = "exclude" in kwargs
        kwargs_include_fields = kwargs.pop("fields", [])
        kwargs_exclude_fields = kwargs.pop("exclude", [])

        # Initialize now that kwargs have been cleared
        super(LimitableSerializer, self).__init__(*args, **kwargs)

        current_viewset = self.context.get("view")

        request = self.context.get("request", None)
        params = {}
        if request:
            params = dict(request.query_params)
            params.update(dict(request.data))

        param_exclude_fields = []
        param_include_fields = []

        # If no kwargs excludes, check the request
        if not kwargs_has_exclude:
            param_exclude_fields = params.get("exclude", [])

        if not kwargs_has_include:
            param_include_fields = params.get("fields", [])

        exclude_fields = []
        include_fields = []

        # Allow excluded and included fields if we are not verbose
        if not self.context.get("verbose", True) or not params.get("verbose", False):
            # Otherwise, use the fields from the lists
            exclude_fields = param_exclude_fields + kwargs_exclude_fields

            if len(exclude_fields) == 0 and hasattr(self.Meta, "default_exclude"):
                exclude_fields = self.Meta.default_exclude

            # If we're not in a detail view, we can use the include lists
            if not current_viewset or current_viewset.action != "retrieve":
                include_fields = param_include_fields + kwargs_include_fields

                if len(include_fields) == 0 and hasattr(self.Meta, "default_fields"):
                    include_fields = self.Meta.default_fields

                # For the include list, we need to include the parent field of
                # any nested fields
                include_fields = include_fields + self.identify_missing_children(self.Meta.model, include_fields)

        # Create and initialize the child serializers
        if hasattr(self.Meta, "nested_serializers"):
            # Initialize the child serializers
            children = self.Meta.nested_serializers
            for field in children.keys():
                # Get child kwargs
                kwargs = children[field].get("kwargs", {})

                # Pop the default fields from the child serializer kwargs
                default_fields = kwargs.pop("default_fields", [])
                default_exclude = kwargs.pop("default_exclude", [])

                child_include_fields, matched_include = self.get_child_fields(field, include_fields)
                child_exclude_fields, matched_exclude = self.get_child_fields(field, exclude_fields)

                # If the excluded field belongs to a child, we cannot include it in the parent
                # exclusion, otherwise we would run into a KeyError
                exclude_fields = [x for x in exclude_fields if x not in matched_exclude]

                if len(child_include_fields) == 0:
                    child_include_fields = default_fields
                if len(child_exclude_fields) == 0:
                    child_exclude_fields = default_exclude

                child_args = {
                    **kwargs,
                    "context": {**self.context, "verbose": False},  # Do not verbos-ify child objects
                    "fields": child_include_fields,
                    "exclude": child_exclude_fields,
                }
                self.fields[field] = children[field]["class"](**child_args)

        # Now we alter our own field sets
        # We must exclude before include to avoid conflicts from user error
        for field_name in exclude_fields:
            self.fields.pop(field_name)

        if len(include_fields) > 0:
            allowed = set(include_fields)
            existing = set(self.fields.keys())
            for field_name in existing - allowed:
                # If we have a coded field, always include its description
                if field_name.split("_")[-1] == "description" and "_".join(field_name.split("_")[:-1]) not in allowed:
                    continue
                self.fields.pop(field_name)

    # Returns a list of child names for fields in the field list
    # This is necessary so that if a user requests "recipient__recipient_name"
    # we also include "recipient" so that it is serialized
    def identify_missing_children(self, model, fields):
        children = []
        model_fields = [f.name for f in model._meta.get_fields()]
        for field in fields:
            split = field.split("__")
            if len(split) > 0 and split[0] in model_fields and split[0] not in fields:
                children.append(split[0])

        return children

    # Takes a child's name and a list of fields, and returns a set of fields
    # that belong to that child
    def get_child_fields(self, child, fields):
        # An included field is a child's field if the field begins with that
        # child's name and two underscores
        pattern = "{}__".format(child)

        matched = []
        child_fields = []
        for field in fields:
            if field[: len(pattern)] == pattern:
                child_fields.append(field[len(pattern) :])
                matched.append(field)
        return child_fields, matched

    @classmethod
    def setup_eager_loading(cls, queryset, prefix=""):
        """
        This method will set up prefetch and selected related statements appropriately
        on a specified query set based upon the serializer's nested_serializer parameter
        in the Meta class. It will return the modified queryset.
        The prefix flag is for cascading down to children, that is, when we eager load
        a child of this serializer, we must prefix that child's field name to their field name.
        For example:
        TasSerializer has a nested serializer of toptier_agency with a nested serializer for toptier agency
        Thus, when we prefetch, we want to prefetch 'toptier_agency'
        This prefix flag allows us to accomplish this.
        N.B.: When doing a 1-1 fk relation, select_related() should be used (this join is performed in the SQL);
              When doing a 1-m or m-m relation, prefetch_related() should be used (this join is performed via Python)
        """
        try:
            # Grab the nested serializers (aka children)
            children = cls.Meta.nested_serializers
            for child in children:
                serializer_class = children[child]["class"]
                if (not hasattr(serializer_class, "prefetchable")) or (not serializer_class.prefetchable):
                    continue
                queryset = queryset.prefetch_related(prefix + child)
                # Since the child might have nested serializers, we set up on that too
                queryset = serializer_class.setup_eager_loading(queryset, prefix=prefix + child + "__")
        except AttributeError:
            # We don't have any nested serializers
            pass

        return queryset


class AggregateSerializer(serializers.Serializer):
    def __init__(self, *args, **kwargs):
        super(AggregateSerializer, self).__init__(*args, **kwargs)

        request = self.context.get("request", None)
        params = dict(request.query_params)
        params.update(dict(request.data))

        include_fields = params.get("group")
        if not isinstance(include_fields, list):
            include_fields = [include_fields]

        for field_name in include_fields:
            if field_name is None:
                continue
            self.fields[field_name] = serializers.CharField(required=False)

    item = serializers.CharField(required=False)
    aggregate = serializers.DecimalField(23, 2)
