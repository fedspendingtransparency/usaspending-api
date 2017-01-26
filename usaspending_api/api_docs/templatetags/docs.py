from django import template
from django.conf import settings
import os
import markdown
from mdx_gfm import GithubFlavoredMarkdownExtension


register = template.Library()


@register.simple_tag
def display_docs():
    full_text = open(os.path.join(settings.BASE_DIR, "using_the_api.md")).read()
    body = full_text.split("[//]: # (Begin Content)")[1]
    return markdown.markdown(body, extensions=[GithubFlavoredMarkdownExtension()])


@register.simple_tag
def display_contents():
    full_text = open(os.path.join(settings.BASE_DIR, "using_the_api.md")).read()
    contents = full_text.split("[//]: # (Begin Content)")[0]
    return markdown.markdown(contents, extensions=[GithubFlavoredMarkdownExtension()])
