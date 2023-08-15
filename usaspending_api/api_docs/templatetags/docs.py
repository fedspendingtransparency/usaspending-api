import markdown

from django import template
from django.conf import settings
from mdx_gfm import GithubFlavoredMarkdownExtension


register = template.Library()


@register.simple_tag(takes_context=True)
def display_docs(context):
    contents = get_contents_from_markdown(context.get("markdown", None), index=1)
    return markdown.markdown(contents, extensions=[GithubFlavoredMarkdownExtension()])


@register.simple_tag(takes_context=True)
def display_contents(context):
    contents = get_contents_from_markdown(context.get("markdown", None))
    md = markdown.markdown(contents, extensions=[GithubFlavoredMarkdownExtension()])
    return md


def get_contents_from_markdown(markdown_file=None, split_string="[//]: # (Begin Content)", index=0):
    """
    Splits the markdown file according to a specified string, and, if the data exists, returns the appropriate
    markdown data.
    """
    contents = ""
    if markdown_file:
        try:
            full_text = open(str(settings.APP_DIR / "api_docs" / "markdown" / markdown_file)).read()
            split = full_text.split(split_string)
            if index < len(split):
                contents = split[index]
        except Exception:
            pass
    return contents
