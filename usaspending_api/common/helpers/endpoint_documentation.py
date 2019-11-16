import os
import re

from usaspending_api.settings import BASE_DIR

# Typically we only care about v2 API endpoints, but if we ever add v3 or
# whatever, add the base path to this tuple.
CURRENT_ENDPOINT_PREFIXES = ("/api/v2/",)

ENDPOINTS_MD = "usaspending_api/api_docs/markdown/endpoints.md"

# This should match stuff like "|[display](url)|method|description|"
ENDPOINT_PATTERN = re.compile(r"\|\s*\[[^\]]+\]\s*\((?P<url>[^)]+)\)\s*\|[^|]+\|[^|]+\|")


def case_sensitive_file_exists(file_path):
    """
    File names are case insensitive on Macs and Windows and case sensitive on
    Linux.  This is one way to perform case sensitive file checking on a case
    insensitive file system.
    """
    directory, filename = os.path.split(file_path)
    return os.path.isfile(file_path) and filename in os.listdir(directory)


def get_endpoint_urls_doc_paths_and_docstrings(endpoint_prefixes=None):
    """
    Compiles the list of endpoint URLs and associated RegexURLPattern objects
    serviced by Django.  If path_prefixes is supplied, returned URLS will be
    restricted to those that start with any of the provided prefixes (must be
    a tuple).

    Returns [("url", <RegexURLPattern object>), ...]
    """
    from usaspending_api import urls

    results = []

    def _traverse_urls(base, url_patterns):
        for url in url_patterns:
            cleaned = base + url.regex.pattern.lstrip("^").rstrip("$")
            if hasattr(url, "url_patterns"):
                _traverse_urls(cleaned, url.url_patterns)
            elif not endpoint_prefixes or cleaned.startswith(endpoint_prefixes):
                results.append((cleaned, url))

    _traverse_urls("/", urls.urlpatterns)

    return results


def get_endpoints_from_endpoints_markdown():
    """
    Looks for and extracts URLs for patterns like |[display](url)|method|description|
    from the master endpoints.md markdown file.
    """
    with open(str(BASE_DIR / ENDPOINTS_MD)) as f:
        contents = f.read()
    return [e.split("?")[0] for e in ENDPOINT_PATTERN.findall(contents) if e]


def get_fully_qualified_name(obj):
    """
    Fully qualifies an object name.  For example, would return
    "usaspending_api.common.helpers.endpoint_documentation.get_fully_qualified_name"
    for this function.
    """
    return "{}.{}".format(obj.__module__, obj.__qualname__)


def validate_docs(url, url_object, master_endpoint_list):
    """
    Ensures that an endpoint_doc property and a docstring is provided for the
    view associated with the provided url and checks that the URL is mentioned
    in the master endpoints.md doc.
    """
    qualified_name = url_object.lookup_str
    view_class = url_object.callback.cls

    messages = []

    if qualified_name == "usaspending_api.common.views.RemovedEndpointView":
        return messages

    if not hasattr(view_class, "endpoint_doc"):
        messages.append("{} ({}) missing endpoint_doc property".format(qualified_name, url))
    else:
        endpoint_doc = getattr(view_class, "endpoint_doc")
        if not endpoint_doc:
            messages.append("{}.endpoint_doc ({}) is invalid".format(qualified_name, url))
        else:
            absolute_endpoint_doc = str(BASE_DIR / endpoint_doc)
            if not case_sensitive_file_exists(absolute_endpoint_doc):
                messages.append(
                    "{}.endpoint_doc ({}) references a file that does not exist ({})".format(
                        qualified_name, url, endpoint_doc
                    )
                )

    if not (view_class.__doc__ or "").strip():
        messages.append("{} ({}) has no docstring".format(qualified_name, url))

    # Fix up the url a little so we can pattern match it.
    pattern = url + ("" if url.endswith("/") else "/") + "?"  # Optional trailing slash.

    # See if we can find anything in the master endpoint list that matches this pattern.
    for endpoint in master_endpoint_list:
        if re.fullmatch(pattern, endpoint):
            break
    else:
        messages.append("No URL found in {} that matches {} ({})".format(ENDPOINTS_MD, url, qualified_name))

    return messages
