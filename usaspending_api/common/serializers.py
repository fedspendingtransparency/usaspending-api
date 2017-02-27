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

        # Check if this is a retrieve method; if so, children will not be
        # serialized unless otherwise specified and all fields will be displayed
        current_viewset = self.context.get('view')
        retrieve_flag = (current_viewset and current_viewset.action == 'retrieve')

        # Create and initialize the child serializers
        try:
            # Initialize the child serializers
            children = self.Meta.nested_serializers
            for field in children.keys():
                child_args = {
                    **children[field].get("kwargs", {}),
                    "context": {**self.context, "child": True}
                }  # The child tag should be removed when child field limiting is implemented
                # If we're 'retreive'ing and the child is not marked as retreivable, reference FK by PK
                if retrieve_flag and not children[field].get("retrieve", False):
                    self.fields[field] = serializers.PrimaryKeyRelatedField(**children[field].get("kwargs", {}))
                else:
                    self.fields[field] = children[field]["class"](**child_args)
        except AttributeError:
            # We don't have any nested serializers
            pass

        request = self.context.get('request')
        if request:
            params = dict(request.query_params)
            params.update(dict(request.data))
            exclude_fields = params.get('exclude')
            include_fields = params.get('fields')

            if params.get('verbose', False):
                # We have a request for verbose, so we return here so that we
                # return all fields
                return

            # We must exclude before include to avoid conflicts from user error
            if exclude_fields is not None and not self.context.get("child", False):  # the child check should be removed when child field limiting is implemented
                for field_name in exclude_fields:
                    try:
                        self.fields.pop(field_name)
                    except KeyError:
                        # Because we're not currently handling nested serializer field
                        # limiting pass-down, this can happen due to the context pass down
                        pass

            if include_fields is not None and not self.context.get("child", False):  # the child check should be removed when child field limiting is implemented
                allowed = set(include_fields)
                existing = set(self.fields.keys())
                for field_name in existing - allowed:
                    try:
                        self.fields.pop(field_name)
                    except KeyError:
                        # Because we're not currently handling nested serializer field
                        # limiting pass-down, this can happen due to the context pass down
                        pass

            elif retrieve_flag:
                # The view has specifically asked that all fields should
                # be returned (for example, when the request url follows
                # the item/{pk} pattern)
                return

            else:
                try:
                    include_fields = self.Meta.model.get_default_fields(path=request._request.path_info)
                    allowed = set(include_fields)
                    existing = set(self.fields.keys())
                    for field_name in existing - allowed:
                        self.fields.pop(field_name)
                except AttributeError:
                    # We don't have get default fields available
                    pass

    @classmethod
    def setup_eager_loading(cls, queryset, prefix="", parent_is_many=False):
        '''
        This method will set up prefetch and selected related statements appropriately
        on a specified query set based upon the serializer's nested_serializer parameter
        in the Meta class. It will return the modified queryset.
        The prefix flag is for cascading down to children, that is, when we eager load
        a child of this serializer, we must prefix that child's field name to their field name.
        For example:
        AwardSerializer has a nested serializer of funding_agency with a nested serializer for toptier agency
        Thus, when we prefetch, we want to prefetch 'funding_agency' and 'funding_agency__toptier_agency'
        This prefix flag allows us to accomplish this.
        N.B.: When doing a 1-1 fk relation, select_related() should be used (this join is performed in the SQL);
              When doing a 1-m or m-m relation, prefetch_related() should be used (this join is performed via Python)
        '''
        try:
            # Grab the nested serializers (aka children)
            children = cls.Meta.nested_serializers
            for child in children:
                is_many = children[child].get("kwargs", {}).get("many", False)
                if is_many or parent_is_many:
                    queryset = queryset.prefetch_related(prefix + child)
                else:
                    queryset = queryset.select_related(prefix + child)
                # Since the child might have nested serializers, we set up on that too
                queryset = children[child]["class"].setup_eager_loading(queryset, prefix=prefix + child + "__", parent_is_many=is_many)
        except AttributeError:
            # We don't have any nested serializers
            pass

        return queryset


class AggregateSerializer(serializers.Serializer):

    item = serializers.CharField(required=False)
    aggregate = serializers.DecimalField(20, 2)
