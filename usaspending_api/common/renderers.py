from rest_framework.renderers import BrowsableAPIRenderer
from django.core.paginator import Page
from rest_framework.request import is_form_media_type, override_method
from django import forms


class BrowsableAPIRendererWithoutForms(BrowsableAPIRenderer):
    """Renders the browsable api, but excludes the HTML form."""

    def get_rendered_html_form(self, data, view, method, request):
        """Don't render the HTML form"""
        return None

    # Lifted from the DRF, but modified to remove content + HTML form
    def get_raw_data_form(self, data, view, method, request):
        """
        Returns a form that allows for arbitrary content types to be tunneled
        via standard HTML forms.
        (Which are typically application/x-www-form-urlencoded)

        Modifications: Set content to None so this doesn't bombard the database
        """
        # See issue #2089 for refactoring this.
        serializer = getattr(data, 'serializer', None)
        if serializer and not getattr(serializer, 'many', False):
            instance = getattr(serializer, 'instance', None)
            if isinstance(instance, Page):
                instance = None
        else:
            instance = None

        with override_method(view, request, method) as request:
            # Check permissions
            if not self.show_form_for_method(view, method, request, instance):
                return

            # If possible, serialize the initial content for the generic form
            default_parser = view.parser_classes[0]
            renderer_class = getattr(default_parser, 'renderer_class', None)
            content = None

            # Generate a generic form that includes a content type field,
            # and a content field.
            media_types = [parser.media_type for parser in view.parser_classes]
            choices = [(media_type, media_type) for media_type in media_types]
            initial = media_types[0]

            class GenericContentForm(forms.Form):
                _content_type = forms.ChoiceField(
                    label='Media type',
                    choices=choices,
                    initial=initial,
                    widget=forms.Select(attrs={'data-override': 'content-type'})
                )
                _content = forms.CharField(
                    label='Content',
                    widget=forms.Textarea(attrs={'data-override': 'content'}),
                    initial=content
                )

            return GenericContentForm()
