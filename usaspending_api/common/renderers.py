from rest_framework.renderers import BrowsableAPIRenderer
from django.core.paginator import Page
from rest_framework.request import override_method
from django import forms
from rest_framework import VERSION
from django.utils.safestring import SafeText
from usaspending_api.settings import BASE_DIR
from collections import OrderedDict
from django.conf import settings
from rest_framework.settings import api_settings


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
        serializer = getattr(data, "serializer", None)
        if serializer and not getattr(serializer, "many", False):
            instance = getattr(serializer, "instance", None)
            if isinstance(instance, Page):
                instance = None
        else:
            instance = None

        with override_method(view, request, method) as request:
            # Check permissions
            if not self.show_form_for_method(view, method, request, instance):
                return

            content = None

            # Generate a generic form that includes a content type field,
            # and a content field.
            media_types = [parser.media_type for parser in view.parser_classes]
            choices = [(media_type, media_type) for media_type in media_types]
            initial = media_types[0]

            class GenericContentForm(forms.Form):
                _content_type = forms.ChoiceField(
                    label="Media type",
                    choices=choices,
                    initial=initial,
                    widget=forms.Select(attrs={"data-override": "content-type"}),
                )
                _content = forms.CharField(
                    label="Content", widget=forms.Textarea(attrs={"data-override": "content"}), initial=content
                )

            return GenericContentForm()


class DocumentApiRenderer(BrowsableAPIRenderer):
    # template = 'rest_framework/doc_api.html'

    def get_context(self, data, accepted_media_type, renderer_context):
        """
        Returns the context used to render.
        """
        view = renderer_context["view"]
        request = renderer_context["request"]
        response = renderer_context["response"]
        renderer = self.get_default_renderer(view)

        raw_data_post_form = self.get_raw_data_form(data, view, "POST", request)
        raw_data_put_form = self.get_raw_data_form(data, view, "PUT", request)
        raw_data_patch_form = self.get_raw_data_form(data, view, "PATCH", request)
        raw_data_put_or_patch_form = raw_data_put_form or raw_data_patch_form

        response_headers = OrderedDict(sorted(response.items()))
        renderer_content_type = ""
        if renderer:
            renderer_content_type = "%s" % renderer.media_type
            if renderer.charset:
                renderer_content_type += " ;%s" % renderer.charset
        response_headers["Content-Type"] = renderer_content_type

        if getattr(view, "paginator", None) and view.paginator.display_page_controls:
            paginator = view.paginator
        else:
            paginator = None

        csrf_cookie_name = settings.CSRF_COOKIE_NAME
        csrf_header_name = getattr(settings, "CSRF_HEADER_NAME", "HTTP_X_CSRFToken")  # Fallback for Django 1.8
        if csrf_header_name.startswith("HTTP_"):
            csrf_header_name = csrf_header_name[5:]
        csrf_header_name = csrf_header_name.replace("_", "-")

        context = {
            "content": self.get_content(renderer, data, accepted_media_type, renderer_context),
            "view": view,
            "request": request,
            "response": response,
            "user": request.user,
            "description": self.get_description(view, response.status_code),
            "name": self.get_name(view),
            "version": VERSION,
            "paginator": paginator,
            "breadcrumblist": self.get_breadcrumbs(request),
            "allowed_methods": view.allowed_methods,
            "available_formats": [renderer_cls.format for renderer_cls in view.renderer_classes],
            "response_headers": response_headers,
            "put_form": self.get_rendered_html_form(data, view, "PUT", request),
            "post_form": self.get_rendered_html_form(data, view, "POST", request),
            "delete_form": self.get_rendered_html_form(data, view, "DELETE", request),
            "options_form": self.get_rendered_html_form(data, view, "OPTIONS", request),
            "filter_form": self.get_filter_form(data, view, request),
            "raw_data_put_form": raw_data_put_form,
            "raw_data_post_form": raw_data_post_form,
            "raw_data_patch_form": raw_data_patch_form,
            "raw_data_put_or_patch_form": raw_data_put_or_patch_form,
            "display_edit_forms": bool(response.status_code != 403),
            "api_settings": api_settings,
            "csrf_cookie_name": csrf_cookie_name,
            "csrf_header_name": csrf_header_name,
        }

        # Inject markdown code
        # markdown_file = open(BASE_DIR + "/usaspending_api/api_docs/api_documentation/Toptier Agencies.md", "r")
        # markdown_text = markdown_file.read()
        # print(markdown_text)
        #
        # context['description'] = markdown_text

        context_array = (context["description"]).split("endpoint_doc: ")
        if len(context_array) > 1:
            context_array[0] = context_array[0].replace("<p>", "")
            context_array[1] = context_array[1].replace("</p>", "")

            git_head_file = open(BASE_DIR + "/.git/HEAD", "r")

            git_branch = str(git_head_file.read()).split("/")[-1]
            git_branch = (
                "https://github.com/fedspendingtransparency/usaspending-api/blob/"
                "{}/usaspending_api/api_docs/api_documentation".format(git_branch)
            )

            doc_location = context_array[1].split()[0]

            path = "{}{}".format(git_branch, str(doc_location))
            path = path.replace("\n", "")
            path = path.replace(" ", "%20")
            doc_description = "<p>{}\n\nDocumentation on this endpoint can be found <a href={}>here<a>.</p>".format(
                context_array[0], path
            )

            context["description"] = SafeText(doc_description)

        return context
