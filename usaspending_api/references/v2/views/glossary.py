



class DefinitionSerializer(LimitableSerializer):

    class Meta:

        model = Definition
        fields = ['term', 'slug', 'data_act_term', 'plain', 'official', 'resources']