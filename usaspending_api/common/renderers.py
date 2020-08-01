import logging

from django import forms
from django.core.paginator import Page
from django.utils.safestring import mark_safe
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.request import override_method
from urllib.parse import quote, urljoin
from usaspending_api.common.helpers.endpoint_documentation import case_sensitive_file_exists
from usaspending_api.settings import REPO_DIR


logger = logging.getLogger("console")


BASE_GITHUB_URL = "https://github.com/fedspendingtransparency/usaspending-api/blob/{git_branch}/usaspending_api"


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


class DocumentAPIRenderer(BrowsableAPIRenderer):
    """
    Add link to the documentation to the endpoint description.
    """

    def get_context(self, data, accepted_media_type, renderer_context):
        """
        Override get_context to insert the documentation link in the description.
        """
        context = super(DocumentAPIRenderer, self).get_context(data, accepted_media_type, renderer_context)

        view = renderer_context.get("view") or context.get("view")
        if view:
            endpoint_doc = getattr(view, "endpoint_doc", None)
            if endpoint_doc:
                git_branch = self._get_current_git_branch("master")
                endpoint_doc = str(REPO_DIR / endpoint_doc)
                endpoint_url = self._build_github_url(endpoint_doc, git_branch)
                description = self._add_url_to_description(context.get("description") or "", endpoint_url)
                context["description"] = mark_safe(description)

        return context

    @staticmethod
    def _get_current_git_branch(default=None):
        """
        Attempt to figure out which git branch is current.  .git/HEAD typically
        follows a structure of refs/heads/<branch name>.  We want <branch name>.
        """
        file_path = str(REPO_DIR / ".git" / "HEAD")
        if case_sensitive_file_exists(file_path):
            with open(file_path) as f:
                return "/".join(f.read().split("/", 2)[2:]).strip()
        return default

    @staticmethod
    def _build_github_url(doc_path, git_branch):
        """
        Convert a file path to a GitHub URL.
        """
        if doc_path.startswith(str(REPO_DIR)):
            doc_path = doc_path[len(str(REPO_DIR)) :].lstrip("/")
            return urljoin(BASE_GITHUB_URL, quote(doc_path)).format(git_branch=git_branch)
        return ""

    @staticmethod
    def _add_url_to_description(description, endpoint_url):
        if endpoint_url:
            return '{}\n<p>Documentation for this endpoint can be found <a href="{}">here</a>.</p>'.format(
                description, endpoint_url
            ).strip()
        return description
