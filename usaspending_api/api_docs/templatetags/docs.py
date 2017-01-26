from django import template
from django.conf import settings
import os
import markdown
from mdx_gfm import GithubFlavoredMarkdownExtension


register = template.Library()


@register.simple_tag
def display_docs():
    return markdown.markdown(open(os.path.join(settings.BASE_DIR, "using_the_api.md")).read(), extensions=[GithubFlavoredMarkdownExtension()])
