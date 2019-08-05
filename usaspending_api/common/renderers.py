import logging
import os
import re

from django import forms
from django.core.paginator import Page
from django.utils.safestring import mark_safe
from html import escape
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.request import override_method
from urllib.parse import quote, urljoin
from usaspending_api.settings import BASE_DIR


logger = logging.getLogger("console")


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


ENDPOINT_DOC_PATTERN = re.compile(r"\b(?P<remove>endpoint_doc:\s*(?P<endpoint_doc>[^\s<]+))")
INFERRED_DOC_PATTERN = re.compile(r"^/api/v\d+/(?P<path>.+?)/?$")
VERSION_PATTERN = re.compile(r"^/api/(?P<version>v\d+)/.+$")
EMPTY_PARAGRAPH_PATTERN = re.compile(r"\s*<p>\s*</p>\s*", re.MULTILINE)

BASE_URL = "https://github.com/fedspendingtransparency/usaspending-api/blob/{git_branch}/usaspending_api"
SEARCH_LOCATIONS = (
    "usaspending_api/api_contracts/contracts/{version}",
    "usaspending_api/api_contracts/contracts",
    "usaspending_api/api_docs/api_documentation",
)


class DocumentAPIRenderer(BrowsableAPIRenderer):
    """
    A new flavor of documentation renderer that injects a link to our full blown
    markdown documentation into the Django Rest Framework endpoint.

    There are two ways to do API endpoint documentation:

        1]  The developer can explicitly add "endpoint_doc:" followed by a
            relative file path to the markdown documentation for the endpoint
            on its own line in the view docstring.  The file path should be
            relative to usaspending_api/api_contracts/contracts/{version}.  See
            other endpoints for examples of how this should look.

        2]  If the "endpoint_doc:" tag does not exist or the document pointed
            to by "endpoint_doc:" does not exist, we will attempt to infer the
            documentation from the API endpoint URL.

    As of this writing, there are three locations where documentation can live:

        -   usaspending_api/api_contracts/contracts/<version> -> We are currently
            in the process of reorganizing our contracts to support versioning
            along with the actual endpoints they document.  This is the preferred
            location.

        -   usaspending_api/api_contracts/contracts -> This is a slightly older
            location for API contracts (prior to versioning) and is deprecated.

        -   usaspending_api/api_docs/api_documentation -> This location holds the
            older style free form markdown documentation and is also deprecated.

    We will search these directories for documentation in the order listed in
    SEARCH_LOCATIONS.
    """
    def get_context(self, *args, **kwargs):
        context = super(DocumentAPIRenderer, self).get_context(*args, **kwargs)

        request_path = self.renderer_context["request"].path

        description, endpoint_doc = self._get_description_and_endpoint_doc(context)
        inferred_doc = self._get_inferred_doc(request_path)
        version = self._get_api_version(request_path)

        doc_path = self._get_doc_path(version, endpoint_doc) or self._get_doc_path(version, inferred_doc)
        doc_url = self._get_github_url(doc_path, self._get_current_git_branch())

        if not doc_url:
            logger.warning("Unable to find documentation for endpoint {}".format(request_path))

        description = self._update_description(description, doc_path)
        description = self._add_url_to_description(description, doc_url)

        context["description"] = mark_safe(description)

        return context

    @staticmethod
    def _get_description_and_endpoint_doc(context):
        """
        Digs the description out of the context, extracts "endpoint_doc" if there is
        one, cleans up the description a little, and returns (description, endpoint_doc)
        """
        description = context.get("description") or ""
        endpoint_doc = ""
        match = ENDPOINT_DOC_PATTERN.search(description)
        if match:
            endpoint_doc = match.group("endpoint_doc")
            description = description[:match.start("remove")] + description[match.end("remove"):]
        description = EMPTY_PARAGRAPH_PATTERN.sub(" ", description).strip()
        return description, endpoint_doc

    @staticmethod
    def _get_inferred_doc(request_path):
        """
        Using the request URL path, generate an inferred documentation path.  If
        all goes well

            usaspending_api/api_contracts/contracts/v2/download/accounts

        should turn into something like

            download/accounts.md

        """
        match = INFERRED_DOC_PATTERN.search(request_path)
        return (match.group("path") + ".md") if match else ""

    @staticmethod
    def _get_api_version(request_path):
        """
        Using the request URL path, let's figure out which version of the API
        we're dealing with here.  API URLs usually resemble

            usaspending_api/api_contracts/contracts/v2/download/accounts

        where the version number in this case is "v2".
        """
        match = VERSION_PATTERN.search(request_path)
        return match.group("version") if match else ""

    @staticmethod
    def _get_doc_path(version, doc):
        """
        Given an API version and a candidate relative doc path, let's see if
        we can find a file that meets our needs.
        """
        if doc:
            file = os.path.split(doc)[1]
            for location in SEARCH_LOCATIONS:
                # Don't attempt to check a location where a version
                # is required but we don't have one.
                if "{version}" not in location or version:
                    doc_path = os.path.join(BASE_DIR, location.format(version=version), doc)
                    # This is stupid.  This is a workaround for case insensitive file systems.
                    if os.path.exists(doc_path) and file in os.listdir(os.path.dirname(doc_path)):
                        return doc_path
        return ""

    @staticmethod
    def _get_current_git_branch():
        """
        This is a bit of legacy code to figure out which git branch is current.
        It works and I don't know a better way so I'm leaving it... for now...
        """
        file_path = os.path.join(BASE_DIR, ".git/HEAD")
        with open(file_path) as f:
            return f.read().split("/")[-1].strip()

    @staticmethod
    def _get_github_url(doc_path, git_branch):
        """
        Convert a file path to a GitHub URL.
        """
        if doc_path.startswith(BASE_DIR):
            doc_path = doc_path[len(BASE_DIR):].lstrip("/")
            return urljoin(BASE_URL, quote(doc_path)).format(git_branch=git_branch)
        return ""

    @staticmethod
    def _update_description(description, doc_path):
        """
        If the view does not have its own docstring, let's see if we can figure
        one out from the markdown file located at doc_path.  We will only check
        the first 10 lines for something that might be a docstring.
        """
        if not doc_path or description:
            return description

        lines = []
        with open(doc_path) as f:
            for n, line in enumerate(f):
                line = line.strip()
                if line and not line.startswith(("FORMAT", "HOST", "#", "+", "*", "-")):
                    lines.append(line)
                elif n > 10 or lines:
                    break

        return "<p>{}</p>".format(escape(" ".join(lines))) if lines else ""

    @staticmethod
    def _add_url_to_description(description, doc_url):
        if doc_url:
            return description + '\n<p>Documentation for this endpoint can be found <a href="{}">here</a>.</p>'.format(
                doc_url
            )
        return description
